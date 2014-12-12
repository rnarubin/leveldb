/**
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.iq80.leveldb.impl;

import org.iq80.leveldb.util.CloseableByteBuffer;
import org.iq80.leveldb.util.ConcurrentZeroCopyWriter;
import org.iq80.leveldb.util.LongToIntFunction;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.SliceInput;
import org.iq80.leveldb.util.SliceOutput;
import org.iq80.leveldb.util.Slices;

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.iq80.leveldb.impl.LogConstants.BLOCK_SIZE;
import static org.iq80.leveldb.impl.LogConstants.HEADER_SIZE;

public abstract class LogWriter
        implements Closeable
{
    private final File file;
    private final long fileNumber;
    private final AtomicBoolean closed = new AtomicBoolean();

    protected LogWriter(File file, long fileNumber)
    {
        Preconditions.checkNotNull(file, "file is null");
        Preconditions.checkArgument(fileNumber >= 0, "fileNumber is negative");

        this.file = file;
        this.fileNumber = fileNumber;
    }

    public boolean isClosed()
    {
        return closed.get();
    }

    public void close()
            throws IOException
    {
        closed.set(true);
    }

    public void delete()
    {
        closed.set(true);

        // try to delete the file
        getFile().delete();
    }

    public File getFile()
    {
        return file;
    }

    public long getFileNumber()
    {
        return fileNumber;
    }
    
    protected void buildRecord(final SliceInput sliceInput) throws IOException
    {
        // Fragment the record into chunks as necessary and write it.  Note that if record
        // is empty, we still want to iterate once to write a single
        // zero-length chunk.

        try(CloseableLogBuffer cbb = getWriter().requestSpace(new LongToIntFunction()
           {
              @Override
              public int applyAsInt(long previousWrite)
              {
                 return calculateWriteSize(previousWrite, sliceInput);
              }
           }))
        {
           // used to track first, middle and last blocks
           boolean begin = true;
           int blockOffset = (int) (cbb.lastEndPosition % BLOCK_SIZE);
           do {
               int bytesRemainingInBlock = BLOCK_SIZE - blockOffset;
               Preconditions.checkState(bytesRemainingInBlock >= 0);

               // Switch to a new block if necessary
               if (bytesRemainingInBlock < HEADER_SIZE) {
                   if (bytesRemainingInBlock > 0) {
                       // Fill the rest of the block with zeros
                       // todo lame... need a better way to write zeros
                       cbb.buffer.put(new byte[bytesRemainingInBlock]);
                   }
                   blockOffset = 0;
                   bytesRemainingInBlock = BLOCK_SIZE - blockOffset;
               }

               // Invariant: we never leave less than HEADER_SIZE bytes available in a block
               int bytesAvailableInBlock = bytesRemainingInBlock - HEADER_SIZE;
               Preconditions.checkState(bytesAvailableInBlock >= 0);

               // if there are more bytes in the record then there are available in the block,
               // fragment the record; otherwise write to the end of the record
               boolean end;
               int fragmentLength;
               if (sliceInput.available() > bytesAvailableInBlock) {
                   end = false;
                   fragmentLength = bytesAvailableInBlock;
               }
               else {
                   end = true;
                   fragmentLength = sliceInput.available();
               }

               // determine block type
               LogChunkType type;
               if (begin && end) {
                   type = LogChunkType.FULL;
               }
               else if (begin) {
                   type = LogChunkType.FIRST;
               }
               else if (end) {
                   type = LogChunkType.LAST;
               }
               else {
                   type = LogChunkType.MIDDLE;
               }

               // write the chunk
               Preconditions.checkArgument(blockOffset + HEADER_SIZE <= BLOCK_SIZE);
               blockOffset += appendChunk(cbb.buffer, type, sliceInput.readSlice(fragmentLength));

               // we are no longer on the first chunk
               begin = false;
           }
           while (sliceInput.isReadable());
        }
    }

    private static int appendChunk(ByteBuffer buffer, LogChunkType type, Slice slice)
    {
        final int length = slice.length();
        Preconditions.checkArgument(length <= 0xffff, "length %s is larger than two bytes", length);

        int crc = Logs.getChunkChecksum(type.getPersistentId(), slice.getRawArray(), slice.getRawOffset(), length);

        // Format the header
        buffer.putInt(crc);
        buffer.put((byte) (length & 0xff));
        buffer.put((byte) (length >>> 8));
        buffer.put((byte) (type.getPersistentId()));

        buffer.put(slice.toByteBuffer());

        return HEADER_SIZE + slice.length();
    }

    public void addRecord(Slice record, boolean sync)
            throws IOException
    {
       Preconditions.checkState(isClosed(), "Log is closed");
       
       buildRecord(record.input());
       
       if(sync)
          sync();
    }
    
    abstract ConcurrentZeroCopyWriter<CloseableLogBuffer> getWriter();
    abstract void sync() throws IOException;
 
    /**
     * calculates the size of this write's data within the log file given the previous writes position,
     * taking into account new headers and padding around block boundaries
     * @param previousWrite end position of the last record submitted for writing
     * @param newData slice with new data to be written
     * @return this write's size in bytes
     */
    public int calculateWriteSize(long previousWrite, SliceInput newData)
    {
       int firstBlockWrite;
       int dataRemaining = newData.available();
       int remainingInBlock = BLOCK_SIZE - (int)(previousWrite%BLOCK_SIZE);
       if(remainingInBlock < HEADER_SIZE){
          //zero fill
          firstBlockWrite = remainingInBlock;
       }
       else{
          remainingInBlock -= (firstBlockWrite = HEADER_SIZE);
          if(remainingInBlock >= dataRemaining)
          {
             //everything fits into the first block
             return firstBlockWrite + dataRemaining;
          }
          firstBlockWrite += remainingInBlock;
          dataRemaining -= remainingInBlock;
       }
       //all subsequent data goes into new blocks
       final int headerCount = dataRemaining / (BLOCK_SIZE - HEADER_SIZE) + 1;
       final int newBlockWrite = dataRemaining + (headerCount * HEADER_SIZE);

       return newBlockWrite + firstBlockWrite;
    }
    
    protected abstract static class CloseableLogBuffer extends CloseableByteBuffer
    {
       private final long lastEndPosition;
       protected CloseableLogBuffer(ByteBuffer buffer, long endPosition)
       {
          super(buffer);
          this.lastEndPosition = endPosition;
       }
    }
}
