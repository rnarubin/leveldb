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
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.atomic.AtomicLong;

import org.iq80.leveldb.Deallocator;
import org.iq80.leveldb.Env;
import org.iq80.leveldb.LongToIntFunction;
import org.iq80.leveldb.MemoryManager;
import org.iq80.leveldb.util.Closeables;
import org.iq80.leveldb.util.SizeOf;

public class MMapEnv
		  extends FileChannelEnv
        implements Env
{
    public MMapEnv()
    {
	     super(null);//uses of the memory manager are all overriden
    }

    @Override
    public MultiWriteFile openMultiWriteFile(Path path)
            throws IOException
    {
        return new FileChannelMultiWriteFile(path);
    }

    @Override
    public SequentialWriteFile openSequentialWriteFile(Path path)
            throws IOException
    {
        return new FileChannelSequentialWriteFile(path);
    }

    @Override
    public SequentialReadFile openSequentialReadFile(Path path)
            throws IOException
    {
        return new FileChannelReadFile(path, memory);
    }

    @Override
    public RandomReadFile openRandomReadFile(Path path)
            throws IOException
    {
        return new FileChannelReadFile(path, memory);
    }

	 private static final class SingleMMapReadFile
	 	extends FileChannelFile
		implements SequentialReadFile, RandomReadFile
    {
	 	private final MappedByteBuffer buffer;
	 	public SingleMMapReadFile(FileChannel channel) throws IOException{
			super(channel);
			long fileSize = channel.size();
			assert fileSize <= Integer.MAX_VALUE : "cannot map more than integer max in single buffer";
			buffer = channel.map(MapMode.READ_ONLY, 0, (int)fileSize);
		}

		@Override
		public int read(ByteBuffer dst) throws IOException{
			
		}

		@Override
		public ByteBuffer read(long position, int length)
		{
			if(position > data.limit() || length <= 0){
				return ByteBuffers.EMPTY_BUFFER;
			}
			return ByteBuffers.duplicate(buffer, (int)position, Math.min(position + length, buffer.limit()));
		}

		@Override
		public void close() throws IOException
		{
			try{
				ByteBuffers.unmap(buffer);
			}
			finally{
				super.close();
			}
		}
	 }

    private static final class FileChannelReadFile
            extends FileChannelFile
            implements SequentialReadFile, RandomReadFile
    {
        private final MemoryManager memory;

        public FileChannelReadFile(Path path, MemoryManager memory)
                throws IOException
        {
            super(FileChannel.open(path, StandardOpenOption.READ));
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

		  private static final Deallocation NOOP_DEALLOCATOR = new Deallocator(){
		  	@Override
			public final void free(ByteBuffer b){
			}
		  };

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
            channel.position(channel.position() + n);
        }
    }

    private static final class FileChannelSequentialWriteFile
            extends FileChannelFile
            implements SequentialWriteFile
    {
        public FileChannelSequentialWriteFile(Path path)
                throws IOException
        {
            super(FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.APPEND));
        }

        @Override
        public int write(ByteBuffer src)
                throws IOException
        {
            return channel.write(src);
        }

        @Override
        public void sync()
                throws IOException
        {
            channel.force(false);
        }

        @Override
        public long size()
                throws IOException
        {
            return channel.size();
        }
    }

    private static final class FileChannelMultiWriteFile
            extends FileChannelFile
            implements MultiWriteFile
    {
        private final AtomicLong filePosition;
        public FileChannelMultiWriteFile(Path path)
                throws IOException
        {
            super(FileChannel.open(path, StandardOpenOption.WRITE));
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
