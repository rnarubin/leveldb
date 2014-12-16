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

import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.util.ByteBufferSupport;
import org.iq80.leveldb.util.ConcurrentNonCopyWriter;
import org.iq80.leveldb.util.Slice;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

public class MMapLogWriter
        extends LogWriter
{
    private static final int PAGE_SIZE = 1024 * 1024;

    public MMapLogWriter(File file, long fileNumber)
            throws IOException
    {
        super(file, fileNumber);
    }

    // Writes a stream of chunks such that no chunk is split across a block boundary
    public void addRecord(Slice record, boolean sync)
            throws IOException
    {
        Preconditions.checkState(!isClosed(), "Log has been closed");
        throw new UnsupportedOperationException("need to fix mmap");
    }

   @Override
   ConcurrentNonCopyWriter<CloseableLogBuffer> getWriter()
   {
      // TODO Auto-generated method stub
      return null;
   }

   @Override
   void sync() throws IOException
   {
      // TODO Auto-generated method stub
      
   }

    /*
    private static class AsyncMMapWriter
            extends AsyncWriter
    {
        private final FileChannel fileChannel;
        private MappedByteBuffer mappedByteBuffer;
        private long fileOffset;

        public AsyncMMapWriter(FileChannel channel, BackgroundExceptionHandler bgExceptionHandler, ThrottlePolicy throttlePolicy)
                throws IOException
        {
            super(bgExceptionHandler, throttlePolicy);
            this.fileChannel = channel;
            this.mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, 0, PAGE_SIZE);
        }

        public MappedByteBuffer getMappedByteBuffer()
        {
            return mappedByteBuffer;
        }

        @Override
        protected long write(List<ByteBuffer[]> b){ throw new UnsupportedOperationException();}
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
    */
}
