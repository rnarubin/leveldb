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

import com.google.common.base.Preconditions;

import org.iq80.leveldb.util.ByteBufferSupport;
import org.iq80.leveldb.util.Slice;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MMapLogWriter
        extends LogWriter
{
    private static final int PAGE_SIZE = 1024 * 1024;

    private final AsyncMMapWriter mmapWriter;

    public MMapLogWriter(File file, long fileNumber)
            throws IOException
    {
        super(file, fileNumber, new AsyncMMapWriter(new RandomAccessFile(file, "rw").getChannel()));
        this.mmapWriter = (AsyncMMapWriter) super.asyncWriter;
    }

    // Writes a stream of chunks such that no chunk is split across a block boundary
    public void addRecord(Slice record, boolean synchronous)
            throws IOException
    {
        Preconditions.checkState(!isClosed(), "Log has been closed");

        Future<Long> write = mmapWriter.submit(buildRecord(record.input()));
        if (synchronous) {
            try {
                write.get();
            }
            catch (InterruptedException ignored) {
            }
            catch (ExecutionException e) {
                throw new IOException("Failed to write to log", e);
            }
            mmapWriter.getMappedByteBuffer().force();
        }
    }

    private static class AsyncMMapWriter
            extends AsyncWriter
    {
        private final FileChannel fileChannel;
        private MappedByteBuffer mappedByteBuffer;
        private long fileOffset;

        public AsyncMMapWriter(FileChannel channel)
                throws IOException
        {
            this.fileChannel = channel;
            this.mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, 0, PAGE_SIZE);
        }

        public MappedByteBuffer getMappedByteBuffer()
        {
            return mappedByteBuffer;
        }

        @Override
        protected long write(ByteBuffer[] buffers)
                throws IOException
        {
            int totalLength = 0;
            for (ByteBuffer b : buffers) {
                totalLength += b.remaining();
            }
            ensureCapacity(totalLength);
            for (ByteBuffer b : buffers) {
                mappedByteBuffer.put(b);
            }

            return totalLength;
        }

        @Override
        public void close()
                throws IOException
        {
            try {
                super.close();
            }
            finally {
                destroyMappedByteBuffer();
                if (fileChannel.isOpen()) {
                    fileChannel.truncate(fileOffset);
                }
            }
        }

        private void ensureCapacity(int bytes)
                throws IOException
        {
            if (mappedByteBuffer.remaining() < bytes) {
                // remap
                fileOffset += mappedByteBuffer.position();
                unmap();

                mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, fileOffset, PAGE_SIZE);
            }
        }

        private void unmap()
        {
            ByteBufferSupport.unmap(mappedByteBuffer);
        }

        private void destroyMappedByteBuffer()
        {
            if (mappedByteBuffer != null) {
                fileOffset += mappedByteBuffer.position();
                unmap();
            }
            mappedByteBuffer = null;
        }
    }
}
