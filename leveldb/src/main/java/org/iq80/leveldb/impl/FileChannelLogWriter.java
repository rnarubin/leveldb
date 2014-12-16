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

import com.google.common.base.Preconditions;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

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
      protected CloseableLogBuffer getBuffer(final long position, final int length)
      {
         return new CloseableFileLogBuffer(position, length);
      }
      
      private class CloseableFileLogBuffer extends CloseableLogBuffer
      {
         private IOException encounteredException = null;
         private long position, limit;
         protected CloseableFileLogBuffer(long endPosition, int length)
         {
            super(endPosition);
            this.position = endPosition;
            this.limit = this.position + length;
         }

         @Override
         public CloseableByteBuffer put(byte b)
         {
            throw new UnsupportedOperationException();
         }
         @Override
         public CloseableByteBuffer put(byte[] b)
         {
            return put(ByteBuffer.wrap(b));
         }

         @Override
         public CloseableByteBuffer put(ByteBuffer b)
         {
            try
            {
               int remaining = b.remaining();
               Preconditions.checkArgument(position + remaining <= limit, "Buffer put exceeds requested space");
               do
               {
                  position += fileChannel.write(b, position);
               }
               while(b.remaining() > 0);
            }
            catch (IOException e)
            {
               encounteredException = e;
            }
            
            return this;
         }
         @Override
         public CloseableByteBuffer putInt(int b)
         {
            throw new UnsupportedOperationException();
         }

         @Override
         public void close() throws IOException
         {
            if(encounteredException != null)
            {
               throw encounteredException;
            }
         }
      }
       
    }

}