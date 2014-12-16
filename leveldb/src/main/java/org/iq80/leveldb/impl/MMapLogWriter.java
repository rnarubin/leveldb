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


import org.iq80.leveldb.util.ByteBufferSupport;
import org.iq80.leveldb.util.CloseableByteBuffer;
import org.iq80.leveldb.util.ConcurrentNonCopyWriter;
import org.iq80.leveldb.util.SizeOf;
import org.iq80.leveldb.util.Slice;

import com.google.common.base.Preconditions;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

public class MMapLogWriter
        extends LogWriter
{
    private static final int PAGE_SIZE = 1024 * 1024;
    private final FileChannel fileChannel;
    private final ConcurrentMMapWriter writer;

    public MMapLogWriter(File file, long fileNumber)
            throws IOException
    {
        super(file, fileNumber);
        this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
        this.writer = new ConcurrentMMapWriter();
    }

   @Override
   ConcurrentNonCopyWriter<CloseableLogBuffer> getWriter()
   {
      throw new UnsupportedOperationException("mmap not yet implemented");
      //return this.writer;
   }

   @Override
   void sync() throws IOException
   {

   }

    private class ConcurrentMMapWriter extends ConcurrentNonCopyWriter<CloseableLogBuffer>
    {
       private MappedRegion region;

       ConcurrentMMapWriter() throws IOException
       {
          region = new MappedRegion(0);
       }

       @Override
       protected CloseableLogBuffer getBuffer(final long position, final int length)
       {
          //return new CloseableMMapLogBuffer(position, ByteBuffer.allocate(length));
          return null;
       }
      
       private class MappedRegion
       {
          public final long position;
          private final MappedByteBuffer mmap;
          MappedRegion(long position) throws IOException
          {
             this.position = position;
             this.mmap = fileChannel.map(MapMode.READ_WRITE, position, PAGE_SIZE);
          }
          
          public CloseableMMapLogBuffer slice(long position, int length)
          {
             ByteBuffer ret = mmap.duplicate();
             ret.position((int)(this.position - position));
             ret.limit(ret.position() + length);
             return new CloseableMMapLogBuffer(position, ret);
          }

          private class CloseableMMapLogBuffer extends CloseableLogBuffer
          {
             private ByteBuffer slice;
             protected CloseableMMapLogBuffer(long endPosition, ByteBuffer slice)
             {
                super(endPosition);
                this.slice = slice;
             }
          
             @Override
             public CloseableByteBuffer put(byte b)
             {
                slice.put(b);
                return this;
             }
             @Override
             public CloseableByteBuffer putInt(int b)
             {
                slice.putInt(b);
                return this;
             }
          
             @Override
             public CloseableByteBuffer put(byte[] b)
             {
                slice.put(b);
                return this;
             }
          
             @Override
             public CloseableByteBuffer put(ByteBuffer b)
             {
                slice.put(b);
                return this;
             }
          
             @Override
             public void close() throws IOException
             {
                slice = null;
             }
          }
       }
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
