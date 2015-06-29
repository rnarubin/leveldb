/*
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

import org.iq80.leveldb.util.Closeables;
import org.iq80.leveldb.util.LongToIntFunction;

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

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
        Closeables.closeQuietly(this);
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

    protected abstract CloseableLogBuffer requestSpace(LongToIntFunction len)
            throws IOException;

    private void buildRecord(final ByteBuffer input)
            throws IOException
    {
        // Fragment the record into chunks as necessary and write it.  Note that if record
        // is empty, we still want to iterate once to write a single
        // zero-length chunk.

        try (CloseableLogBuffer buffer = requestSpace(new LongToIntFunction()
        {
            @Override
            public int applyAsInt(long previousWrite)
            {
                return calculateWriteSize(previousWrite, input);
            }
        })) {
            // used to track first, middle and last blocks
            boolean begin = true;
            int blockOffset = (int) (buffer.lastEndPosition % BLOCK_SIZE);
            do {
                int bytesRemainingInBlock = BLOCK_SIZE - blockOffset;
                assert (bytesRemainingInBlock >= 0);

                // Switch to a new block if necessary
                if (bytesRemainingInBlock < HEADER_SIZE) {
                    if (bytesRemainingInBlock > 0) {
                        // Fill the rest of the block with zeros
                        // todo lame... need a better way to write zeros
                        buffer.put(new byte[bytesRemainingInBlock]);
                    }
                    blockOffset = 0;
                    bytesRemainingInBlock = BLOCK_SIZE - blockOffset;
                }

                // Invariant: we never leave less than HEADER_SIZE bytes available in a block
                int bytesAvailableInBlock = bytesRemainingInBlock - HEADER_SIZE;
                assert (bytesAvailableInBlock >= 0);

                // if there are more bytes in the record then there are available in the block,
                // fragment the record; otherwise write to the end of the record
                boolean end;
                int fragmentLength;
                if (input.remaining() > bytesAvailableInBlock) {
                    end = false;
                    fragmentLength = bytesAvailableInBlock;
                }
                else {
                    end = true;
                    fragmentLength = input.remaining();
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
                final int oldlim = input.limit();
                input.limit(input.position() + fragmentLength);
                blockOffset += appendChunk(buffer, type, input);
                input.limit(oldlim);

                // we are no longer on the first chunk
                begin = false;
            }
            while (input.hasRemaining());
        }
    }

    private static int appendChunk(CloseableLogBuffer buffer, LogChunkType type, ByteBuffer data)
            throws IOException
    {
        final int length = data.remaining();
        assert (length <= 0xffff) : String.format("length %s is larger than two bytes", length);

        int crc = Logs.getChunkChecksum(type.getPersistentId(), data);

        // Format the header
        buffer.putInt(crc);
        buffer.put((byte) (length & 0xff));
        buffer.put((byte) (length >>> 8));
        buffer.put((byte) (type.getPersistentId()));

        buffer.put(data);

        return HEADER_SIZE + length;
    }

    public final void addRecord(ByteBuffer record, boolean sync)
            throws IOException
    {
        Preconditions.checkState(!isClosed(), "Log is closed");

        buildRecord(record);

        if (sync) {
            sync();
        }
    }

    protected abstract void sync()
            throws IOException;

    /**
     * calculates the size of this write's data within the log file given the previous writes position,
     * taking into account new headers and padding around block boundaries
     *
     * @param previousWrite end position of the last record submitted for writing
     * @param newData slice with new data to be written
     * @return this write's size in bytes
     */
    public static int calculateWriteSize(long previousWrite, ByteBuffer newData)
    {
        int firstBlockWrite;
        int dataRemaining = newData.remaining();
        int remainingInBlock = BLOCK_SIZE - (int) (previousWrite % BLOCK_SIZE);
        if (remainingInBlock < HEADER_SIZE) {
            //zero fill
            firstBlockWrite = remainingInBlock;
        }
        else {
            remainingInBlock -= (firstBlockWrite = HEADER_SIZE);
            if (remainingInBlock >= dataRemaining) {
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

    protected abstract static class CloseableLogBuffer
            implements Closeable
    {
        private final long lastEndPosition;

        protected CloseableLogBuffer(long startPosition)
        {
            this.lastEndPosition = startPosition;
        }

        public abstract CloseableLogBuffer put(byte b)
                throws IOException;

        public abstract CloseableLogBuffer put(byte[] b)
                throws IOException;

        public abstract CloseableLogBuffer put(ByteBuffer b)
                throws IOException;

        public abstract CloseableLogBuffer putInt(int b)
                throws IOException;
    }
}
