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
import org.iq80.leveldb.util.SizeOf;

import com.google.common.base.Preconditions;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
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
    

    private class ConcurrentFileWriter extends ConcurrentNonCopyWriter<CloseableLogBuffer>
    {
       private final ScratchBuffer scratchCache[] = new ScratchBuffer[64];
       private final AtomicLong scratchBitSet = new AtomicLong(0xFFFFFFFFFFFFFFFFL);
       
       ConcurrentFileWriter()
       {
          ByteBuffer cache = ByteBuffer.allocateDirect(64 * SIZE_OF_LONG);
          for(int i = 0, pos = 0; i < scratchCache.length; i++, pos += SIZE_OF_LONG){
             cache.position(pos);
             ByteBuffer subset = cache.slice().order(ByteOrder.LITTLE_ENDIAN);
             subset.limit(subset.position() + SIZE_OF_LONG);
             scratchCache[i] = new ScratchBuffer(i, subset);
          }
          
       }
    
       private ScratchBuffer getScratchBuffer()
       {
          long state, bitIndex;
          do{
             state = scratchBitSet.get();
             if((bitIndex = Long.lowestOneBit(state)) == 0)
             {
                //no scratch space available in cache, allocate heap buffer
                return new ScratchBuffer(-1, ByteBuffer.allocate(SizeOf.SIZE_OF_LONG).order(ByteOrder.LITTLE_ENDIAN));
             }
          } while(!scratchBitSet.compareAndSet(state, state & (~bitIndex)));
          final int cacheIndex = Long.numberOfTrailingZeros(bitIndex);
          return scratchCache[cacheIndex];
       }
       
       private void releaseScratchBuffer(ScratchBuffer scratch)
       {
          if(scratch.index < 0)
             return; //heap allocated, do nothing

          long state;
          do{
             state = scratchBitSet.get();
          } while(!scratchBitSet.compareAndSet(state, state | (1 << scratch.index)));
       }
       
       private class ScratchBuffer
       {
          private final int index;
          public final ByteBuffer buffer;
          ScratchBuffer(int index, ByteBuffer buffer)
          {
             this.index = index;
             this.buffer = buffer;
          }
       }

      protected CloseableLogBuffer getBuffer(final long position, final int length)
      {
         return new CloseableFileLogBuffer(position, length);
      }
      
      private class CloseableFileLogBuffer extends CloseableLogBuffer
      {
         private IOException encounteredException = null;
         private long position;
         private final long limit;
         //use scratch space and a minimum flush size to improve writing of headers
         private ScratchBuffer scratch = getScratchBuffer();
         private final static int writeMin = 8;
         protected CloseableFileLogBuffer(long endPosition, int length)
         {
            super(endPosition);
            this.position = endPosition;
            this.limit = this.position + length;
         }

         @Override
         public CloseableByteBuffer put(byte b)
         {
            scratch.buffer.put(b);
            if(scratch.buffer.position() >= writeMin)
            {
               putScratch();
            }
            return this;
         }
         @Override
         public CloseableByteBuffer putInt(int b)
         {
            scratch.buffer.putInt(b);
            if(scratch.buffer.position() >= writeMin)
            {
               putScratch();
            }
            return this;
         }
         
         private void putScratch()
         {
            scratch.buffer.flip();
            put(scratch.buffer);
            scratch.buffer.clear();
         }

         @Override
         public CloseableByteBuffer put(byte[] b)
         {
            return put(ByteBuffer.wrap(b));
         }

         @Override
         public CloseableByteBuffer put(ByteBuffer b)
         {
            if(scratch.buffer.position() > 0)
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
            releaseScratchBuffer(scratch);
            scratch = null; //help gc, but also crash in case of use after close rather than undefined behavior, or close checks everywhere
            if(encounteredException != null)
            {
               throw encounteredException;
            }
         }
      }
       
    }

}