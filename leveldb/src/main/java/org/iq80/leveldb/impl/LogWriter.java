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
    
    private final AtomicReference<WritePosition> lastWrite = new AtomicReference<>(new WritePosition(0, 0));

    protected WriteRecord buildRecord(SliceInput sliceInput)
    {
        WritePosition newWrite = new WritePosition();
        WritePosition previousWrite;
        do {
           // the way that data is split along block boundaries is dependent on the last data's position
           previousWrite = lastWrite.get();
        }
        //depending on the size of input data, the calculation performed in the CAS could take varying amounts of time.
        //this can lead to a lot of wasted cycles in high contention cases when the calculation is performed but the CAS fails.
        //generally speaking, small input data or data that is unlikely to span multiple blocks can be calculated quickly.
        //even with contention and wasted cycles, this is often better than synchronized performance
        while(!lastWrite.compareAndSet(previousWrite, newWrite.recalculate(previousWrite, sliceInput)));
        WriteRecord record = new WriteRecord(previousWrite.getEndPosition(), (int) (newWrite.getEndPosition() - previousWrite.getEndPosition()));

        // Fragment the record into chunks as necessary and write it.  Note that if record
        // is empty, we still want to iterate once to write a single
        // zero-length chunk.

        // used to track first, middle and last blocks
        boolean begin = true;
        int blockOffset = previousWrite.getBlockOffset();
        do {
            int bytesRemainingInBlock = BLOCK_SIZE - blockOffset;
            Preconditions.checkState(bytesRemainingInBlock >= 0);

            // Switch to a new block if necessary
            if (bytesRemainingInBlock < HEADER_SIZE) {
                if (bytesRemainingInBlock > 0) {
                    // Fill the rest of the block with zeros
                    // todo lame... need a better way to write zeros
                    record.add(ByteBuffer.allocate(bytesRemainingInBlock));
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
            blockOffset += appendChunk(record, type, sliceInput.readSlice(fragmentLength));

            // we are no longer on the first chunk
            begin = false;
        }
        while (sliceInput.isReadable());

        assert newWrite.getBlockOffset() == blockOffset: "calculated block offset and actual block offset mismatch";

        return record;
    }

    private static int appendChunk(WriteRecord record, LogChunkType type, Slice slice)
    {
        Preconditions.checkArgument(slice.length() <= 0xffff, "length %s is larger than two bytes", slice.length());

        // create header
        Slice header = newLogRecordHeader(type, slice, slice.length());


        //it's much cheaper to return an array of buffers
        //rather than create a new buffer and copy the contents
        record.add(header.toByteBuffer());
        record.add(slice.toByteBuffer());
        return HEADER_SIZE + slice.length();
    }

    private static Slice newLogRecordHeader(LogChunkType type, Slice slice, int length)
    {
        int crc = Logs.getChunkChecksum(type.getPersistentId(), slice.getRawArray(), slice.getRawOffset(), length);

        // Format the header
        SliceOutput header = Slices.allocate(HEADER_SIZE).output();
        header.writeInt(crc);
        header.writeByte((byte) (length & 0xff));
        header.writeByte((byte) (length >>> 8));
        header.writeByte((byte) (type.getPersistentId()));

        return header.slice();
    }

    abstract void addRecord(Slice record, boolean sync)
            throws IOException;
    
    private static class WritePosition
    {
       private int blockOffset;
       private long endPosition;
       
       public WritePosition()
       {
          this(0, 0);
       }
       
       public WritePosition(int blockOffset, long endPosition)
       {
          this.blockOffset = blockOffset;
          this.endPosition = endPosition;
       }
       
       public int getBlockOffset()
       {
          return this.blockOffset;
       }
       
       public long getEndPosition()
       {
          return this.endPosition;
       }
       
       /**
        * calculates this write's ending blockOffset within the last record block and the absolute position of the end of its data within the log file
        * @param previousWrite WritePosition belonging to the preceding record submitted for writing
        * @param newData slice with new data to be written
        * @return this
        */
       public WritePosition recalculate(WritePosition previousWrite, SliceInput newData)
       {
          int firstBlockWrite;
          int dataRemaining = newData.available();
          int remainingInBlock = BLOCK_SIZE - previousWrite.blockOffset;
          if(remainingInBlock < HEADER_SIZE){
             //zero fill
             firstBlockWrite = remainingInBlock;
          }
          else{
             firstBlockWrite = HEADER_SIZE;
             remainingInBlock -= HEADER_SIZE;
             if(remainingInBlock >= dataRemaining)
             {
                //everything fits into the first block
                firstBlockWrite += dataRemaining;
                this.blockOffset = previousWrite.blockOffset + firstBlockWrite;
                this.endPosition = previousWrite.endPosition + firstBlockWrite;
                return this;
             }
             firstBlockWrite += remainingInBlock;
             dataRemaining -= remainingInBlock;
          }
          //all subsequent data goes into new blocks
          final int headerCount = dataRemaining / (BLOCK_SIZE - HEADER_SIZE) + 1;
          final int newBlockWrite = dataRemaining + (headerCount * HEADER_SIZE);

          this.blockOffset = newBlockWrite % BLOCK_SIZE;
          this.endPosition = previousWrite.endPosition + newBlockWrite + firstBlockWrite;
          return this;
       }
    }
    
    protected static class WriteRecord
    {
       final long startPosition;
       final int length;
       final List<ByteBuffer> data;
       
       public WriteRecord(long startPosition, int length)
       {
          this.startPosition = startPosition;
          this.length = length;
          this.data = new ArrayList<>();
       }
       
       public void add(ByteBuffer buffer)
       {
          this.data.add(buffer);
       }

       public long getStartPosition()
       {
          return startPosition;
       }
       
       public int getLength()
       {
          return length;
       }
       
       public List<ByteBuffer> getData()
       {
          return data;
       }
       
    }
}
