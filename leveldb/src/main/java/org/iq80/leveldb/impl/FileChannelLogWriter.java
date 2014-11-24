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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class FileChannelLogWriter
        extends LogWriter
{
   
    private final FileChannel fileChannel;
    private final AtomicLong filePosition;

    public FileChannelLogWriter(File file, long fileNumber)
            throws IOException
    {
       super(file, fileNumber);
       this.fileChannel = new FileOutputStream(file, true).getChannel();
       this.filePosition = new AtomicLong(fileChannel.position());
    }

    // Writes a stream of chunks such that no chunk is split across a block boundary
    @Override
    public void addRecord(Slice record, boolean sync)
            throws IOException
    {
        Preconditions.checkState(!isClosed(), "Log has been closed");

        List<ByteBuffer> toWrite = buildRecord(record.input());
        long length = 0;
        for(ByteBuffer b:toWrite)
        {
           length += b.remaining();
        }
        //advance the position atomically so that subsequent writes can proceed while this thread fills in its reserved space
        long position = filePosition.getAndAdd(length);
        for(ByteBuffer b:toWrite)
        {
           position += fileChannel.write(b, position);
        }
        
        if(sync)
        {
           fileChannel.force(false);
        }
    
    }

}