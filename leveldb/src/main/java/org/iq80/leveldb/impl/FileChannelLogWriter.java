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

import org.iq80.leveldb.ThrottlePolicy;
import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.impl.DbImpl.BackgroundExceptionHandler;
import org.iq80.leveldb.util.Slice;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class FileChannelLogWriter
        extends LogWriter
{
    private final AsyncFileWriter fileWriter;

    /**
     * @param file
     * @param fileNumber
     * @param bgExceptionHandler - if null, will throw exceptions in async writer thread; these will only propagate if {@link WriteOptions#sync(boolean) sync()} is set to true
     * @param throttlePolicy - if null, will use {@link ThrottlePolicies#noThrottle() ThrottlePolicies.noThrottle()}
     */
    public FileChannelLogWriter(File file, long fileNumber, BackgroundExceptionHandler bgExceptionHandler, ThrottlePolicy throttlePolicy)
            throws FileNotFoundException
    {
        super(file, fileNumber, new AsyncFileWriter(new FileOutputStream(file).getChannel(), bgExceptionHandler, throttlePolicy));
        //a minor inconvenience, but i can't pass to super a variable that i
        // initialize here, so this ends up being a bit ugly
        this.fileWriter = (AsyncFileWriter) super.asyncWriter;
    }

    // Writes a stream of chunks such that no chunk is split across a block boundary
    @Override
    public void addRecord(Slice record, boolean sync)
            throws IOException
    {
        Preconditions.checkState(!isClosed(), "Log has been closed");

        Future<Long> write = fileWriter.submit(buildRecord(record.input()));
        if (sync) {
            try {
                write.get();
            }
            catch (InterruptedException ignored) {
            }
            catch (ExecutionException e) {
                throw new IOException("Failed to write to log file", e);
            }
            fileWriter.getChannel().force(false);
        }
    }

    private static class AsyncFileWriter
            extends AsyncWriter
    {
        private final FileChannel channel;

        public AsyncFileWriter(FileChannel channel, BackgroundExceptionHandler bgExceptionHandler, ThrottlePolicy throttlePolicy)
        {
            super(bgExceptionHandler, throttlePolicy);
            this.channel = channel;
        }

        public FileChannel getChannel()
        {
            return channel;
        }

        @Override
        protected long write(ByteBuffer[] buffers)
                throws IOException
        {
            return channel.write(buffers);
        }

        @Override
        public void close()
                throws IOException
        {
            try {
                super.close();
            }
            finally {
                channel.force(true);
                channel.close();
            }
        }
    }
}
