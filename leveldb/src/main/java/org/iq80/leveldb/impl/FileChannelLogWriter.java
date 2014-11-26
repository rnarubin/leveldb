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

import org.iq80.leveldb.util.Slice;

import com.google.common.base.Preconditions;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

public class FileChannelLogWriter
        extends LogWriter
{
   
    private final FileChannel fileChannel;

    public FileChannelLogWriter(File file, long fileNumber)
            throws IOException
    {
       super(file, fileNumber);
       this.fileChannel = new FileOutputStream(file, true).getChannel();
    }

    // Writes a stream of chunks such that no chunk is split across a block boundary
    @Override
    public void addRecord(Slice record, boolean sync)
            throws IOException
    {
        Preconditions.checkState(!isClosed(), "Log has been closed");

        WriteRecord toWrite = buildRecord(record.input());
        long position = toWrite.getStartPosition();
        for(ByteBuffer buffer:mergeBuffers(toWrite.getData()))
        {
           position += fileChannel.write(buffer, position);
        }
        
        if(sync)
        {
           fileChannel.force(false);
        }
    
    }
    
    private static final int MERGE_THRESHOLD = 1024;
    /**
     * copy individual adjacent buffers under a certain size threshold into a single contiguous buffer
     * used to lessen the number of system calls made via filechannel.write
     */
    private static List<ByteBuffer> mergeBuffers(List<ByteBuffer> buffers)
    {
       if(buffers.size() <= 1)
       {
          return buffers;
       }

       List<ByteBuffer> newBuffers = new ArrayList<>();
       Iterator<ByteBuffer> iter = buffers.iterator();
       while(iter.hasNext()){
          ByteBuffer b = iter.next();
          if(b.remaining() < MERGE_THRESHOLD){
             int mergeLength = b.remaining();
             Queue<ByteBuffer> toMerge = new ArrayDeque<>();
             toMerge.add(b);
             ByteBuffer next = null;
             while(iter.hasNext() && (next = iter.next()).remaining() < MERGE_THRESHOLD){
                mergeLength += next.remaining();
                toMerge.add(next);
                next = null;
             }
             if(toMerge.size() > 1){
                ByteBuffer merge = ByteBuffer.allocate(mergeLength);
                for(; toMerge.size() > 0; merge.put(toMerge.poll()));
                merge.flip();
                newBuffers.add(merge);
             }
             else{
                //no adjacent buffers meet merge criteria, add buffer unchanged
                newBuffers.add(b);
             }
             if(next != null){
                newBuffers.add(next);
             }
          }
          else{
             newBuffers.add(b);
          }
       }
       return newBuffers;
    }

}