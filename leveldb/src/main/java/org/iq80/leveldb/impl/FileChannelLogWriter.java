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

import org.iq80.leveldb.util.LongToIntFunction;
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
    private final AtomicLong filePosition = new AtomicLong(0);

    @SuppressWarnings("resource")
    public FileChannelLogWriter(File file, long fileNumber)
            throws IOException
    {
        super(file, fileNumber);
        this.fileChannel = new FileOutputStream(file, false).getChannel();
    }

    @Override
    protected void sync()
            throws IOException
    {
        fileChannel.force(false);
    }

    @Override
    public void close()
            throws IOException
    {
        super.close();
        fileChannel.close();
    }

    @Override
    public CloseableLogBuffer requestSpace(LongToIntFunction getLength)
    {
        long state;
        int length;
        do {
            state = filePosition.get();
        }
        while (!filePosition.compareAndSet(state, state + (length = getLength.applyAsInt(state))));

        return new CloseableFileLogBuffer(state, length);
    }

    private static final ThreadLocal<ByteBuffer> scratch = new ThreadLocal<ByteBuffer>()
    {
        @Override
        public synchronized ByteBuffer initialValue()
        {
            return ByteBuffer.allocateDirect(4096).order(ByteOrder.LITTLE_ENDIAN);
        }
    };

    private class CloseableFileLogBuffer
            extends CloseableLogBuffer
    {
        private long position;
        private final long limit;
        // use scratch space and a minimum flush size to improve small write performance
        private ByteBuffer buffer = scratch.get();

        protected CloseableFileLogBuffer(final long startPosition, final int length)
        {
            super(startPosition);
            this.position = startPosition;
            this.limit = startPosition + length;
        }

        @Override
        public CloseableLogBuffer put(byte b)
                throws IOException
        {
            checkCapacity(SizeOf.SIZE_OF_BYTE);
            buffer.put(b);
            return this;
        }

        @Override
        public CloseableLogBuffer putInt(int b)
                throws IOException
        {
            checkCapacity(SizeOf.SIZE_OF_INT);
            buffer.putInt(b);
            return this;
        }

        private void checkCapacity(int size)
                throws IOException
        {
            if (size > buffer.remaining()) {
                buffer.flip();
                write(buffer);
                buffer.clear();
            }
        }

        @Override
        public CloseableLogBuffer put(byte[] b)
                throws IOException
        {
            checkCapacity(b.length);
            if (b.length > buffer.remaining()) {
                write(ByteBuffer.wrap(b));
            }
            else {
                buffer.put(b);
            }
            return this;
        }

        @Override
        public CloseableLogBuffer put(ByteBuffer b)
                throws IOException
        {
            checkCapacity(b.remaining());
            if (b.remaining() > buffer.remaining()) {
                write(b);
            }
            else {
                buffer.put(b);
            }
            return this;
        }

        private void write(ByteBuffer b)
                throws IOException
        {
            if (position + b.remaining() > limit) {
                throw new BufferOverflowException();
            }

            while (b.remaining() > 0) {
                position += fileChannel.write(b, position);
            }
        }

        @Override
        public void close()
                throws IOException
        {
            buffer.flip();
            write(buffer);
            buffer.clear();
        }
    }
}
