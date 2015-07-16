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
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import sun.nio.ch.FileChannelImpl;

import org.iq80.leveldb.Deallocator;
import org.iq80.leveldb.Env;
import org.iq80.leveldb.util.ByteBuffers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("restriction")
public class MMapEnv
        extends FileChannelEnv
        implements Env
{
    public static final Env INSTANCE = new MMapEnv();
    private static final Logger LOGGER = LoggerFactory.getLogger(MMapEnv.class);

    // could consider smaller values to evaluate performance characteristics
    private static final long MMAP_SIZE_LIMIT = Integer.MAX_VALUE;

    public MMapEnv()
    {
        super(null);// uses of the memory manager are all overriden
    }

    // @Override
    // public MultiWriteFile openMultiWriteFile(Path path)
    // throws IOException
    // {
    // }
    //
    // @Override
    // public SequentialWriteFile openSequentialWriteFile(Path path)
    // throws IOException
    // {
    // return new MMapSequentialWriteFile(path);
    // }

    @Override
    public SequentialReadFile openSequentialReadFile(Path path)
            throws IOException
    {
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        return channel.size() > MMAP_SIZE_LIMIT ? new MultiMMapSequentialReadFile(channel) : new SingleMMapReadFile(
                channel);
    }

    @Override
    public RandomReadFile openRandomReadFile(Path path)
            throws IOException
    {
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        return channel.size() > MMAP_SIZE_LIMIT ? new MultiMMapRandomReadFile(channel)
                : new SingleMMapReadFile(channel);
    }

    private static final Method unmap;
    static {
        Method x;
        try {
            x = FileChannelImpl.class.getDeclaredMethod("unmap", MappedByteBuffer.class);
            x.setAccessible(true);
        }
        catch (Throwable t) {
            LOGGER.debug("failed to access MappedByteBuffer support", t);
            x = null;
        }
        unmap = x;
    }

    public static void unmap(MappedByteBuffer buffer)
            throws IOException
    {
        if (unmap != null && buffer != null) {
            try {
                unmap.invoke(null, buffer);
            }
            catch (Exception e) {
                throw new IOException("Failed to unmap MappedByteBuffer", e);
            }
        }
        // else
        // leave it to Java GC
    }

    private static final Deallocator NOOP_DEALLOCATOR = new Deallocator()
    {
        @Override
        public final void free(ByteBuffer b)
        {
        }
    };

    private static final class SingleMMapReadFile
            extends FileChannelFile
            implements SequentialReadFile, RandomReadFile
    {
        private final MappedByteBuffer mmap;
        private final ByteBuffer buffer;

        public SingleMMapReadFile(FileChannel channel)
                throws IOException
        {
            super(channel);
            long fileSize = channel.size();
            assert fileSize <= Integer.MAX_VALUE : "cannot map more than integer max in single buffer";
            this.mmap = channel.map(MapMode.READ_ONLY, 0, (int) fileSize);
            this.mmap.order(ByteOrder.LITTLE_ENDIAN);
            this.buffer = ByteBuffers.duplicate(mmap);
        }

        @Override
        public int read(ByteBuffer dst)
                throws IOException
        {
            int read;
            if (dst.remaining() <= buffer.remaining()) {
                read = dst.remaining();
                int oldlim = buffer.limit();
                buffer.limit(buffer.position() + read);
                dst.put(buffer);
                buffer.limit(oldlim);
            }
            else {
                read = buffer.remaining();
                if (read == 0) {
                    return -1; // EOF
                }
                dst.put(buffer);
            }

            return read;
        }

        @Override
        public ByteBuffer read(long position, int length)
        {
            if (position > buffer.limit() || length <= 0) {
                return ByteBuffers.EMPTY_BUFFER;
            }
            return ByteBuffers.duplicate(buffer, (int) position, (int) Math.min(position + length, buffer.limit()));
        }

        @Override
        public void close()
                throws IOException
        {
            try {
                unmap(mmap);
            }
            finally {
                super.close();
            }
        }

        @Override
        public Deallocator deallocator()
        {
            return NOOP_DEALLOCATOR;
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
            buffer.position((int) Math.min(buffer.limit(), buffer.position() + n));
        }
    }

    private static final int PAGE_SIZE = 1 << 20;

    private static final class MultiMMapSequentialReadFile
            extends FileChannelFile
            implements SequentialReadFile
    {
        private final long fileSize;
        private long position;
        private MappedByteBuffer mmap;

        public MultiMMapSequentialReadFile(FileChannel channel)
                throws IOException
        {
            super(channel);
            fileSize = channel.size();
            position = channel.position();
            remap();
            throw new UnsupportedOperationException();
        }

        private void remap()
                throws IOException
        {
            if (mmap != null) {
                position += mmap.capacity() - mmap.remaining();
                unmap(mmap);
            }
            mmap = channel.map(MapMode.READ_ONLY, position, (int) Math.min(PAGE_SIZE, fileSize - position));
            mmap.order(ByteOrder.LITTLE_ENDIAN);
        }

        @Override
        public int read(ByteBuffer dst)
                throws IOException
        {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public void skip(long n)
                throws IOException
        {
            // TODO Auto-generated method stub

        }

        @Override
        public void close()
                throws IOException
        {
            try {
                unmap(mmap);
            }
            finally {
                super.close();
            }
        }
    }

    private static final class MultiMMapRandomReadFile
            extends FileChannelFile
            implements RandomReadFile
    {
        public MultiMMapRandomReadFile(FileChannel channel)
        {
            super(channel);
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteBuffer read(long position, int length)
                throws IOException
        {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Deallocator deallocator()
        {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public long size()
                throws IOException
        {
            // TODO Auto-generated method stub
            return 0;
        }
    }

    private static final class MMapSequentialWriteFile
            extends FileChannelFile
            implements SequentialWriteFile
    {
        private MappedByteBuffer mmap;
        private long position;

        public MMapSequentialWriteFile(Path path)
                throws IOException
        {
            super(FileChannel.open(path, StandardOpenOption.READ, // read option necessary for READ_WRITE mapmode
                    StandardOpenOption.WRITE, StandardOpenOption.CREATE));
            position = channel.position();
            remap();
        }

        private void remap()
                throws IOException
        {
            mmap = channel.map(MapMode.READ_WRITE, position, PAGE_SIZE);
            mmap.order(ByteOrder.LITTLE_ENDIAN);
        }

        @Override
        public int write(ByteBuffer src)
                throws IOException
        {
            int written = 0;
            int rem;
            while (src.remaining() > (rem = mmap.remaining())) {
                int oldlim = src.limit();
                src.limit(src.position() + rem);

                written += rem;
                mmap.put(src);
                position += mmap.capacity();
                unmap(mmap);
                remap();

                src.limit(oldlim);
            }
            written += src.remaining();
            mmap.put(src);
            return written;
        }

        @Override
        public void sync()
                throws IOException
        {
            mmap.force();
        }

        @Override
        public long size()
                throws IOException
        {
            return channel.size();
        }

        @Override
        public void close()
                throws IOException
        {
            try {
                unmap(mmap);
            }
            finally {
                try {
                    channel.truncate(position + mmap.position());
                }
                finally {
                    super.close();
                }
            }
        }
    }
}
