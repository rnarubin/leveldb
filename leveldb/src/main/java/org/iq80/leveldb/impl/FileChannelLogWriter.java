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
import org.iq80.leveldb.util.ConcurrentObjectPool;
import org.iq80.leveldb.util.LongToIntFunction;
import org.iq80.leveldb.util.ObjectPool;
import org.iq80.leveldb.util.ObjectPool.PooledObject;
import org.iq80.leveldb.util.SizeOf;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import static org.iq80.leveldb.util.SizeOf.SIZE_OF_LONG;

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
    
    protected void sync() throws IOException
    {
       fileChannel.force(false);
    }
    
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
          final int numCachedBuffers = 64;
          ByteBuffer cache = ByteBuffer.allocateDirect(numCachedBuffers * SIZE_OF_LONG);
          ArrayList<ByteBuffer> bufs = new ArrayList<>(numCachedBuffers);
          for(int i = 0, pos = 0; i < numCachedBuffers; i++, pos += SIZE_OF_LONG){
             cache.position(pos);
             ByteBuffer subset = cache.slice().order(ByteOrder.LITTLE_ENDIAN);
             subset.limit(subset.position() + SIZE_OF_LONG);
             bufs.add(subset);
          }
          scratchCache = ConcurrentObjectPool.fixedAllocatingPool(bufs, new Supplier<ByteBuffer>()
          {
              @Override
              public ByteBuffer get()
              {
                 return ByteBuffer.allocate(SizeOf.SIZE_OF_LONG).order(ByteOrder.LITTLE_ENDIAN);
              }
          });
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
         //use scratch space and a minimum flush size to improve writing of headers
         private PooledObject<ByteBuffer> scratch = scratchCache.acquire();
         private ByteBuffer buffer = scratch.get();
         private final static int writeMin = 8;
         protected CloseableFileLogBuffer(long startPosition, int length)
         {
            super(startPosition);
            this.position = startPosition;
            this.limit = this.position + length;
         }

         @Override
         public CloseableByteBuffer put(byte b)
         {
            buffer.put(b);
            if(buffer.position() >= writeMin)
            {
               putScratch();
            }
            return this;
         }
         @Override
         public CloseableByteBuffer putInt(int b)
         {
            buffer.putInt(b);
            if(buffer.position() >= writeMin)
            {
               putScratch();
            }
            return this;
         }
         
         private void putScratch()
         {
            buffer.flip();
            put(buffer);
            buffer.clear();
         }

         @Override
         public CloseableByteBuffer put(byte[] b)
         {
            return put(ByteBuffer.wrap(b));
         }

         @Override
         public CloseableByteBuffer put(ByteBuffer b)
         {
            if(buffer.position() > 0)
            {
               //scratch hasn't been flushed before a bigger write has come in
               putScratch();
            }
            try
            {
               Preconditions.checkArgument(position + b.remaining() <= limit, "Buffer put exceeds requested space");
               while(b.remaining() > 0)
               {
                  position += fileChannel.write(b, position);
               }
            }
            catch (IOException e)
            {
               encounteredException = e;
            }
            
            return this;
         }

         @Override
         public void close() throws IOException
         {
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