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
import org.iq80.leveldb.util.LongToIntFunction;
import org.iq80.leveldb.util.SizeOf;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicLong;

public class MMapLogWriter
        extends LogWriter
{
    private static final int PAGE_SIZE = 1024 * 1024;
    private final FileChannel fileChannel;
    private final ConcurrentMMapWriter writer;

    @SuppressWarnings("resource")
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
      return this.writer;
   }

   @Override
   void sync() throws IOException
   {
      fileChannel.force(false);
   }
   
   @Override
   public void close() throws IOException
   {
      super.close();
      fileChannel.close();
   }

    private class ConcurrentMMapWriter implements ConcurrentNonCopyWriter<CloseableLogBuffer>
    {
       private long filePosition;
       private volatile MappedRegion region;
       
       ConcurrentMMapWriter() throws IOException
       {
          this.filePosition = 0;
          this.region = new MappedRegion(this.filePosition);
       }

       @Override
       public CloseableLogBuffer requestSpace(final int length) throws IOException
       {
          return requestSpace(new LongToIntFunction()
          {
             public final int applyAsInt(long ignored)
             {
                return length;
             }
          });
       }
       
       @Override
       public CloseableLogBuffer requestSpace(LongToIntFunction getLength) throws IOException
       {
          MappedRegion current;
          int length;
          long offset;
          do {
             current = region;
             offset = current.offset.get();
             if(current.offset.compareAndSet(offset, offset + (length = getLength.applyAsInt(offset)))) {
                //CAS success
                if(offset <= current.limit && offset + length > current.limit) {
                   //this is the first write which does not fit into the currently mapped region
                   if(length > PAGE_SIZE) {
                      //TODO: fragment buffers
                      MultiMappedRegion newRegion = new MultiMappedRegion(offset, length, current);
                      region = newRegion.tail;
                      return newRegion.slice();
                   }
                   else {
                      MappedRegion newRegion = new MappedRegion(offset);
                      newRegion.offset.addAndGet(length);
                      region = newRegion;
                      return newRegion.slice(offset, length);
                   }
                }
                else if(offset < current.limit) {
                   //write fits in current region
                   return current.slice(offset, length);
                }
                //else
                //doesn't fit into region, but wasn't the first to exceed the limit
                //loop back
             }
             //CAS fail, loop back
          } while(true);
       }
       
       /*
        * 0: ready for writing
        * /: reserved by someone else
        * 
        * data: ------------- /////00000///// -------
        *       filePosition ^               ^ limit
        *            buffer offset^     ^buffer limit
        *                          |___|
        *                      length, slice
        *                     |_____________|
        *                        PAGE_SIZE
        *                     |_____________|
        *                         mmapped     
        */
       private class MappedRegion
       {
          public final long filePosition;
          public final long limit;
          public final AtomicLong offset;
          private final MappedByteBuffer mmap;
          MappedRegion(long position) throws IOException
          {
             this.filePosition = position;
             this.offset = new AtomicLong(filePosition);
             this.limit = filePosition + PAGE_SIZE;
             this.mmap = fileChannel.map(MapMode.READ_WRITE, filePosition, PAGE_SIZE);
          }
          
          public CloseableMMapLogBuffer slice(long offset, int length)
          {
             ByteBuffer ret = mmap.duplicate();
             ret.position((int)(offset - filePosition));
             ret.limit(ret.position() + length);
             return new CloseableMMapLogBuffer(offset, ret.order(ByteOrder.LITTLE_ENDIAN));
          }

          private class CloseableMMapLogBuffer extends CloseableLogBuffer
          {
             private ByteBuffer slice;
             protected CloseableMMapLogBuffer(long startPosition, ByteBuffer slice)
             {
                super(startPosition);
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
                //could consider tracking references to slices of this mapped region and manually unmap when everyone has closed.
                //that's not as easy to do thread safely while limiting contention though.
                //as it stands, the JVM unmaps the region at the time of GC (which of course takes care of the reference tracking)
                //so this is only a true concern when operating in a memory constrained environment
                //but in those cases you could turn off leveldb's mmapping (and if memory was really a strict concern you'd probably not use Java)
             }
          }
       }
       
       /*
        * 0: ready for writing
        * +: reserved for writing
        * /: reserved by someone else
        * 
        *      ------------- /////0000000 +++++++++++  +++++++++++  ...  +++++++++++  +++++++++++  00000////// ---------
        * head.filePosition ^           ^ head.limit                             tail.filePosition ^
        *                  offset^                                                                     ^ limit (implicit)
        *                        |_________________________________ ... _______________________________|
        *                                                      length, slice
        *                   |___________||___________||___________| ... |___________||___________||___________|
        *                     PAGE_SIZE    PAGE_SIZE    PAGE_SIZE         PAGE_SIZE    PAGE_SIZE    PAGE_SIZE
        *                   |___________||_________________________ ... _________________________||___________|
        *                         |                       mmap and unmap as we go                       |
        *               mmapped already, head                                               mmap for the next guy, tail
        */
       private class MultiMappedRegion
       {
          public final long offset;
          private final long tailPosition;
          private final long headPosition;
          private ByteBuffer currentSlice;
          private final ByteBuffer tailSlice;
          private MappedRegion tail;
          
          MultiMappedRegion(long offset, int length, MappedRegion head) throws IOException
          {
             this.offset = offset;

             final int remainingInHead = (int)(head.limit - offset);
             this.headPosition = head.filePosition;
             this.currentSlice = head.slice(offset, remainingInHead).slice;

             final int usedInTail = (length - remainingInHead)%PAGE_SIZE;
             this.tailPosition = offset + length - usedInTail;

             this.tail = new MappedRegion(tailPosition);
             this.tailSlice = tail.slice(tailPosition, usedInTail).slice;
             tail.offset.addAndGet(usedInTail);
          }
          
          public CloseableMultiMMapLogBuffer slice()
          {
             return new CloseableMultiMMapLogBuffer();
          }
          
          private class CloseableMultiMMapLogBuffer extends CloseableLogBuffer
          {

            private long mmapPosition;
            protected CloseableMultiMMapLogBuffer()
            {
               super(offset);
               this.mmapPosition = headPosition;
            }

            @Override
            public CloseableByteBuffer put(byte b) throws IOException
            {
               if(SizeOf.SIZE_OF_BYTE > currentSlice.remaining()) {
                  currentSlice = mapNext();
               }
               currentSlice.put(b);
               return this;
            }
            @Override
            public CloseableByteBuffer put(byte[] b) throws IOException
            {
               if(b.length > currentSlice.remaining()) {
                  return put(ByteBuffer.wrap(b));
               }

               currentSlice.put(b);
               return this;
            }
            @Override
            public CloseableByteBuffer putInt(int b) throws IOException
            {
               if(SizeOf.SIZE_OF_INT > currentSlice.remaining()) {
                  return put((ByteBuffer)ByteBuffer.allocate(SizeOf.SIZE_OF_INT).order(ByteOrder.LITTLE_ENDIAN).putInt(b).flip());
               }
                  
               currentSlice.putInt(b);
               return this;
            }
            @Override
            public CloseableByteBuffer put(ByteBuffer b) throws IOException
            {
               int rem;
               if(b.remaining() > (rem = currentSlice.remaining())) {
                  final int oldLimit = b.limit();
                  b.limit(b.position() + rem);
                  currentSlice.put(b);
                  currentSlice = mapNext();
                  b.limit(oldLimit);
                  return put(b);
               }
               
               currentSlice.put(b);
               return this;
            }
            @Override
            public void close() throws IOException
            {
            }
            
            private ByteBuffer mapNext() throws IOException
            {
               assert mmapPosition % PAGE_SIZE == 0;
               if(mmapPosition != headPosition) {
                  ByteBufferSupport.unmap((MappedByteBuffer)currentSlice);
               }

               mmapPosition += PAGE_SIZE;

               if(mmapPosition == tailPosition) {
                  return tailSlice;
               }
               
               if(mmapPosition > tailPosition) {
                  throw new BufferOverflowException();
               }
               
               return fileChannel.map(MapMode.READ_WRITE, mmapPosition, PAGE_SIZE).order(ByteOrder.LITTLE_ENDIAN);
            }
          }
       }
    }
}