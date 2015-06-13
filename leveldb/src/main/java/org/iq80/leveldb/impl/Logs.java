/*
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

import org.iq80.leveldb.Options;
import org.iq80.leveldb.util.ByteBufferCrc32;
import org.iq80.leveldb.util.ByteBuffers;
import org.iq80.leveldb.util.ObjectPool;
import org.iq80.leveldb.util.ObjectPools;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public final class Logs
{
    private Logs()
    {
    }

    public static LogWriter createLogWriter(File file, long fileNumber, Options options)
            throws IOException
    {
        return createLogWriter(file, fileNumber, options, ObjectPools.directBufferPool(16, 4096));
    }

    public static LogWriter createLogWriter(File file,
            long fileNumber,
            Options options,
            ObjectPool<ByteBuffer> scratchCache)
            throws IOException
    {
        switch (options.ioImplemenation()) {
            case MMAP:
                return new MMapLogWriter(file, fileNumber);
            case FILE:
                return new FileChannelLogWriter(file, fileNumber, scratchCache);
            default:
                throw new IllegalArgumentException("Unknown log file implementation:" + options.ioImplemenation());
        }
    }

    public static int getChunkChecksum(int chunkTypeId, ByteBuffer data)
    {
        ByteBufferCrc32 crc32 = ByteBuffers.crc32();
        crc32.update(chunkTypeId);
        crc32.update(data, data.position(), data.remaining());
        return ByteBuffers.maskChecksum(crc32.getIntValue());
    }
}
