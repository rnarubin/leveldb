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

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;

import org.iq80.leveldb.Deallocator;
import org.iq80.leveldb.LongToIntFunction;
import org.iq80.leveldb.MemoryManager;
import org.iq80.leveldb.util.Closeables;
import org.iq80.leveldb.util.SizeOf;

public class FileChannelEnv
        extends FileSystemEnv
{
    protected final MemoryManager memory;

    public FileChannelEnv(MemoryManager memory)
    {
        this(memory, false);
    }

    public FileChannelEnv(MemoryManager memory, boolean legacySST)
    {
        super(legacySST);
        this.memory = memory;
    }

    @Override
    public FileChannelConcurrentWriteFile openMultiWriteFile(Path path)
            throws IOException
    {
        return new FileChannelConcurrentWriteFile(path);
    }

    @Override
    public FileChannelSequentialWriteFile openSequentialWriteFile(Path path)
            throws IOException
    {
        return new FileChannelSequentialWriteFile(path);
    }

    @Override
    public FileChannelTemporaryWriteFile openTemporaryWriteFile(Path temp, Path target)
            throws IOException
    {
        return new FileChannelTemporaryWriteFile(temp, target);
    }

    @Override
    public FileChannelReadFile openSequentialReadFile(Path path)
            throws IOException
    {
        return new FileChannelReadFile(path, memory);
    }

    @Override
    public FileChannelReadFile openRandomReadFile(Path path)
            throws IOException
    {
        return new FileChannelReadFile(path, memory);
    }

    @Override
    public LockFile lockFile(Path path)
            throws IOException
    {
        return new FileChannelLockFile(path);
    }

    protected static abstract class FileChannelFile
            implements Channel
    {
        protected final FileChannel channel;
        protected final Path path;

        protected FileChannelFile(Path path, FileChannel channel)
        {
            this.path = path;
            this.channel = channel;
        }

        @Override
        public final boolean isOpen()
        {
            return channel.isOpen();
        }

        @Override
        public void close()
                throws IOException
        {
            channel.close();
        }

        @Override
        public String toString()
        {
            String name = getClass().getSimpleName();
            return (name.isEmpty() ? getClass().getName() : name) + "[path=" + path + "]";
        }
    }

    protected static class FileChannelLockFile
            extends FileChannelFile
            implements LockFile
    {
        private FileLock fileLock;
        private boolean acquired = false;

        public FileChannelLockFile(Path path)
                throws IOException
        {
            super(path, FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE));
            try {
                fileLock = channel.tryLock();
                acquired = true;
            }
            catch (OverlappingFileLockException e) {
                fileLock = null;
                acquired = false;
            }
            catch (Exception e) {
                Closeables.closeQuietly(channel);
                throw e;
            }
        }

        @Override
        public boolean isValid()
        {
            return acquired && fileLock.isValid();
        }

        @Override
        public void close()
                throws IOException
        {
            try {
                if (fileLock != null) {
                    fileLock.release();
                }
            }
            finally {
                super.close();
            }
        }
    }

    protected static class FileChannelReadFile
            extends FileChannelFile
            implements SequentialReadFile, RandomReadFile
    {
        private final MemoryManager memory;

        public FileChannelReadFile(Path path, MemoryManager memory)
                throws IOException
        {
            super(path, FileChannel.open(path, StandardOpenOption.READ));
            this.memory = memory;
        }

        @Override
        public int read(ByteBuffer dst)
                throws IOException
        {
            return channel.read(dst);
        }

        @Override
        public ByteBuffer read(long position, int length)
                throws IOException
        {
            ByteBuffer ret = memory.allocate(length);
            ret.mark();
            channel.read(ret, position);
            ret.limit(ret.position()).reset();
            return ret;
        }

        @Override
        public Deallocator deallocator()
        {
            return memory;
        }

        @Override
        public long size()
                throws IOException
        {
            return channel.size();
        }

        @Override
        public void skip(long n)
                throws IOException
        {
            channel.position(channel.position() + n);
        }
    }

    protected static class FileChannelSequentialWriteFile
            extends FileChannelFile
            implements SequentialWriteFile
    {
        public FileChannelSequentialWriteFile(Path path)
                throws IOException
        {
            super(path, FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.APPEND,
                    StandardOpenOption.CREATE_NEW));
        }

        @Override
        public final int write(ByteBuffer src)
                throws IOException
        {
            return channel.write(src);
        }

        @Override
        public final void sync()
                throws IOException
        {
            channel.force(false);
        }

        @Override
        public final long size()
                throws IOException
        {
            return channel.size();
        }
    }

    protected static class FileChannelTemporaryWriteFile
            extends FileChannelSequentialWriteFile
            implements TemporaryWriteFile
    {
        private final Path temp, target;
        private boolean saved = false;

        public FileChannelTemporaryWriteFile(Path temp, Path target)
                throws IOException
        {
            super(temp);
            this.temp = temp;
            this.target = target;
        }

        @Override
        public void save()
                throws IOException
        {
            super.close();
            Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            saved = true;
        }

        @Override
        public void close()
                throws IOException
        {
            if (!saved) {
                super.close();
            }
        }
    }

    protected static class FileChannelConcurrentWriteFile
            extends FileChannelFile
            implements ConcurrentWriteFile
    {
        private final AtomicLong filePosition;
        public FileChannelConcurrentWriteFile(Path path)
                throws IOException
        {
            super(path, FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW));
            this.filePosition = new AtomicLong(0);
        }

        @Override
        public WriteRegion requestRegion(LongToIntFunction getSize)
                throws IOException
        {
            long state;
            int length;
            do {
                state = filePosition.get();
            }
            while (!filePosition.compareAndSet(state, state + (length = getSize.applyAsInt(state))));

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

        private final class CloseableFileLogBuffer
                implements WriteRegion
        {
            private long position;
            private final long limit;
            private final long startPosition;
            // use scratch space and a minimum flush size to improve small write
            // performance
            private ByteBuffer buffer = scratch.get();

            protected CloseableFileLogBuffer(final long startPosition, final int length)
            {
                this.startPosition = startPosition;
                this.position = startPosition;
                this.limit = startPosition + length;
            }

            @Override
            public void put(byte b)
                    throws IOException
            {
                checkCapacity(SizeOf.SIZE_OF_BYTE);
                buffer.put(b);
            }

            @Override
            public void putInt(int b)
                    throws IOException
            {
                checkCapacity(SizeOf.SIZE_OF_INT);
                buffer.putInt(b);
            }

            private void checkCapacity(int size)
                    throws IOException
            {
                if (size > buffer.remaining()) {
                    flush();
                }
            }

            @Override
            public void put(ByteBuffer b)
                    throws IOException
            {
                checkCapacity(b.remaining());
                if (b.remaining() > buffer.remaining()) {
                    write(b);
                }
                else {
                    buffer.put(b);
                }
            }

            private void write(ByteBuffer b)
                    throws IOException
            {
                if (position + b.remaining() > limit) {
                    throw new BufferOverflowException();
                }

                while (b.remaining() > 0) {
                    position += channel.write(b, position);
                }
            }

            private void flush()
                    throws IOException
            {
                buffer.flip();
                write(buffer);
                buffer.clear();

            }

            @Override
            public void close()
                    throws IOException
            {
                flush();
            }

            @Override
            public long startPosition()
            {
                return startPosition;
            }

            @Override
            public void sync()
                    throws IOException
            {
                flush();
                channel.force(false);
            }
        }
    }
}
