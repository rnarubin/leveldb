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

import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.util.Closeables;
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

import static org.iq80.leveldb.impl.LogConstants.BLOCK_SIZE;
import static org.iq80.leveldb.impl.LogConstants.HEADER_SIZE;

public abstract class LogWriter
        implements Closeable
{
    private final File file;
    private final long fileNumber;
    private final AtomicBoolean closed = new AtomicBoolean();
    protected final AsyncWriter asyncWriter;

    protected LogWriter(File file, long fileNumber, AsyncWriter asyncWriter)
    {
        Preconditions.checkNotNull(file, "file is null");
        Preconditions.checkArgument(fileNumber >= 0, "fileNumber is negative");

        this.file = file;
        this.fileNumber = fileNumber;
        this.asyncWriter = asyncWriter;
    }

    public boolean isClosed()
    {
        return closed.get();
    }

    public synchronized void close()
            throws IOException
    {
        closed.set(true);
        asyncWriter.close();
    }

    public synchronized void delete()
    {
        closed.set(true);

        Closeables.closeQuietly(asyncWriter);

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

    /**
     * Current offset in the current block
     */
    private int blockOffset;

    protected List<ByteBuffer[]> buildRecord(SliceInput sliceInput)
    {
        // used to track first, middle and last blocks
        boolean begin = true;

        List<ByteBuffer[]> toWrite = new ArrayList<>();
        // Fragment the record int chunks as necessary and write it.  Note that if record
        // is empty, we still want to iterate once to write a single
        // zero-length chunk.
        do {
            int bytesRemainingInBlock = BLOCK_SIZE - blockOffset;
            Preconditions.checkState(bytesRemainingInBlock >= 0);

            // Switch to a new block if necessary
            if (bytesRemainingInBlock < HEADER_SIZE) {
                if (bytesRemainingInBlock > 0) {
                    // Fill the rest of the block with zeros
                    // todo lame... need a better way to write zeros
                    toWrite.add(new ByteBuffer[] {ByteBuffer.allocate(bytesRemainingInBlock)});
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
            toWrite.add(writeChunk(type, sliceInput.readSlice(fragmentLength)));

            // we are no longer on the first chunk
            begin = false;
        }
        while (sliceInput.isReadable());

        return toWrite;
    }

    private ByteBuffer[] writeChunk(LogChunkType type, Slice slice)
    {
        Preconditions.checkArgument(slice.length() <= 0xffff, "length %s is larger than two bytes", slice.length());
        Preconditions.checkArgument(blockOffset + HEADER_SIZE <= BLOCK_SIZE);

        // create header
        Slice header = newLogRecordHeader(type, slice, slice.length());

        blockOffset += HEADER_SIZE + slice.length();

        //it's much cheaper to return an array of buffers
        //rather than create a new buffer and copy the contents
        return new ByteBuffer[] {header.toByteBuffer(), slice.toByteBuffer()};
    }

    private Slice newLogRecordHeader(LogChunkType type, Slice slice, int length)
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

    // Writes a stream of chunks such that no chunk is split across a block boundary
    abstract void addRecord(Slice record, boolean sync)
            throws IOException;
}
