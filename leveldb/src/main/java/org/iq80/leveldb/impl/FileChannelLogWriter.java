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
import org.iq80.leveldb.util.ConcurrentNonCopyWriter;
import org.iq80.leveldb.util.ObjectPools;
import org.iq80.leveldb.util.LongToIntFunction;
import org.iq80.leveldb.util.ObjectPool;
import org.iq80.leveldb.util.ObjectPool.PooledObject;
import org.iq80.leveldb.util.SizeOf;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

public class FileChannelLogWriter
        extends LogWriter
{
   
    private final FileChannel fileChannel;
    private final ConcurrentFileWriter writer;

    @SuppressWarnings("resource")
   public FileChannelLogWriter(File file, long fileNumber)
            throws IOException
    {
       super(file, fileNumber);
       this.fileChannel = new FileOutputStream(file, true).getChannel();
       this.writer = new ConcurrentFileWriter();
    }
    
    protected ConcurrentFileWriter getWriter()
    {
       return this.writer;
    }
    
    @Override
    protected void sync() throws IOException
    {
       fileChannel.force(false);
    }
    
    @Override
    public void close() throws IOException
    {
       super.close();
       fileChannel.close();
    }
    

    private class ConcurrentFileWriter implements ConcurrentNonCopyWriter<CloseableLogBuffer>
    {
       private final AtomicLong position = new AtomicLong(0);
       private final ObjectPool<ByteBuffer> scratchCache;
       
       ConcurrentFileWriter()
       {
          scratchCache = ObjectPools.directBufferPool(64, 1024);
       }
    
      @Override
      public CloseableLogBuffer requestSpace(int length)
      {
         return getBuffer(position.getAndAdd(length), length);
      }

      @Override
      public CloseableLogBuffer requestSpace(LongToIntFunction getLength)
      {
         long state;
         int length;
         do {
            state = position.get();
         } while(!position.compareAndSet(state, state + (length = getLength.applyAsInt(state))));
         
         return getBuffer(state, length);
      }

      private CloseableLogBuffer getBuffer(final long position, final int length)
      {
         return new CloseableFileLogBuffer(position, length);
      }
      
      private class CloseableFileLogBuffer extends CloseableLogBuffer
      {
         private IOException encounteredException = null;
         private long position;
         private final long limit;
         //use scratch space and a minimum flush size to improve small write performance
         private PooledObject<ByteBuffer> scratch = scratchCache.acquire();
         private ByteBuffer buffer = scratch.get().order(ByteOrder.LITTLE_ENDIAN);
         protected CloseableFileLogBuffer(long startPosition, int length)
         {
            super(startPosition);
            this.position = startPosition;
            this.limit = this.position + length;
         }

         @Override
         public CloseableByteBuffer put(byte b)
         {
            checkCapacity(SizeOf.SIZE_OF_BYTE);
            buffer.put(b);
            return this;
         }
         @Override
         public CloseableByteBuffer putInt(int b)
         {
            checkCapacity(SizeOf.SIZE_OF_INT);
            buffer.putInt(b);
            return this;
         }
         
         private void checkCapacity(int size)
         {
            if(size > buffer.remaining())
            {
               buffer.flip();
               write(buffer);
               buffer.clear();
            }
         }

         @Override
         public CloseableByteBuffer put(byte[] b)
         {
            checkCapacity(b.length);
            if(b.length > buffer.remaining())
            {
               write(ByteBuffer.wrap(b));
            }
            else
            {
               buffer.put(b);
            }
            return this;
         }

         @Override
         public CloseableByteBuffer put(ByteBuffer b)
         {
            checkCapacity(b.remaining());
            if(b.remaining() > buffer.remaining() || b.isDirect())
            {
               write(b);
            }
            else
            {
               buffer.put(b);
            }
            return this;
         }
         
         private void write(ByteBuffer b)
         {
            try
            {
               if(position + b.remaining() > limit)
                  throw new BufferOverflowException();

               while(b.remaining() > 0)
               {
                  position += fileChannel.write(b, position);
               }
            }
            catch (IOException e)
            {
               encounteredException = e;
            }
            
         }

         @Override
         public void close() throws IOException
         {
            buffer.flip();
            write(buffer);
            buffer = null;
            scratch.close();
            if(encounteredException != null)
            {
               throw encounteredException;
            }
         }
      }
       
    }

}