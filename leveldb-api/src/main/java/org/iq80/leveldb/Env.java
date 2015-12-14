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

package org.iq80.leveldb;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.iq80.leveldb.Env.ConcurrentWriteFile.WriteRegion;

/**
 * An Env is an interface used by the leveldb implementation to access operating
 * system functionality like the filesystem etc. Callers may wish to provide a
 * custom Env object when opening a database to get fine gain control; e.g., to
 * utilize SMR drives
 */
public interface Env
{
    /**
     * Creates and opens a new {@link ConcurrentWriteFile} for the given file
     * info. If a file already exists with this info, the existing file is first
     * deleted.
     */
    ConcurrentWriteFile openMultiWriteFile(FileInfo info)
            throws IOException;

    /**
     * Creates and opens a new {@link SequentialWriteFile} with the given file
     * info. If a file already exists with this info, the existing file is first
     * deleted.
     */
    SequentialWriteFile openSequentialWriteFile(FileInfo info)
            throws IOException;

    /**
     * Creates and opens a new {@link TemporaryWriteFile} with the given temp
     * file info. If a file already exists with the temp info, the existing file
     * is first deleted. Upon closing, the data in the temp file should be
     * atomically saved to the target file info, replacing the target file if it
     * exists
     */
    TemporaryWriteFile openTemporaryWriteFile(FileInfo temp, FileInfo target)
            throws IOException;

    /**
     * Opens an existing {@link SequentialReadFile} with the given file info.
     *
     * @throws FileNotFoundException
     *             if a file with this info does not exist
     */
    SequentialReadFile openSequentialReadFile(FileInfo info)
            throws IOException;

    /**
     * Opens an existing {@link RandomReadFile} with the given file info.
     *
     * @throws FileNotFoundException
     *             if a file with this info does not exist
     */
    RandomReadFile openRandomReadFile(FileInfo info)
            throws IOException;

    /**
     * Deletes the file with the given path
     *
     * @throws FileNotFoundException
     *             if a file with this info does not exist
     */
    void deleteFile(FileInfo info)
            throws IOException;

    /**
     * @return true iff a file with the given file info exists; false iff the
     *         file does not exist. Otherwise, i.e. the file existence cannot be
     *         determined, throw an IOException describing the issue
     */
    boolean fileExists(FileInfo info)
            throws IOException;

    /**
     * Creates a directory to contain all files owned by this database if one
     * does not already exist. Does nothing if it does exist
     *
     * @param existing
     *            a handle to an already existing database. Can be null,
     *            indicating the database does not yet exist or its existence
     *            has not been determined
     * @return a handle to the created database
     */
    DBHandle createDBDir(DBHandle existing)
            throws IOException;

    /**
     * Deletes the specified database directory and all files contained in it
     */
    void deleteDir(DBHandle handle)
            throws IOException;

    /**
     * Returns an iterable over the files owned by the given DB
     */
    Iterable<FileInfo> getOwnedFiles(DBHandle handle)
            throws IOException;

    /**
     * A handle used to identify a directory containing files for a particular
     * database
     */
    /*
     * the right thing to do here would actually be use a generic type parameter
     * instead of this handle. the trouble is that that type would have to be
     * woven into DbImpl, Env, FileInfo, Options, etc, at which point the gain
     * of type safety in the already rare case of using a non-default env is
     * outweighed by code simplicity
     */
    public interface DBHandle
    {
    }

    /**
     * Lock the specified file. Used to prevent concurrent access to the same db
     * by multiple processes. The caller should call LockFile.close() to release
     * the lock. If the process exits, the lock should be automatically
     * released.
     *
     * If somebody else already holds the lock, finishes immediately, i.e. this
     * call does not wait for existing locks to go away.
     *
     * May create the named file if it does not already exist.
     *
     * @return a LockFile object associated with the file. Calling
     *         {@link LockFile#isValid()} will indicate whether the file was
     *         successfully locked
     */
    LockFile lockFile(FileInfo info)
            throws IOException;

    public interface LockFile
            extends Closeable
    {
        /**
         * Indicates whether the file was successfully locked by this object
         */
        public boolean isValid();

        /**
         * Close the file, releasing its associated lock in the process
         */
        @Override
        public void close()
                throws IOException;
    }

    /**
     * A file to which multiple writers may attempt to append concurrently. Safe
     * concurrency is facilitated by the space request mechanism: each writer
     * will provide a function which, given the current end position of the
     * file, will return the amount of space this writer will use. This amount
     * of space should be exclusively reserved for the writer beginning at the
     * given position
     * <p>
     * An example implementation may maintain an internal buffer, utilizing an
     * atomically updated offset within the buffer as the logical end of file.
     * Multiple writers may then insert into the buffer simultaneously. The
     * buffer can be periodically flushed to the file at its physical end
     * position
     * </p>
     * <p>
     * Alternatively, a simple implementation may maintain a mutex which must be
     * acquired by each writer. This would allow each write to perform a
     * conventional file append, although a backing buffer for each
     * {@link WriteRegion} is recommended as writers may submit small data
     * fragments at a time
     */
    public interface ConcurrentWriteFile
            extends Closeable
    {
        /**
         * Given the current position of the file, return a {@link WriteRegion}
         * of the specified size beginning at that position for use by a single
         * writer
         *
         * @param getSize
         *            a function which applied to the current end position of
         *            the file returns the amount of space that the writer will
         *            use
         */
        WriteRegion requestRegion(LongToIntFunction getSize)
                throws IOException;

        /**
         * A region of the {@link ConcurrentWriteFile} which has been
         * exclusively reserved by a single writer
         */
        public interface WriteRegion
                extends Closeable
        {
            /**
             * @return the position within the file at which this region begins
             */
            long startPosition();

            void put(byte b)
                    throws IOException;

            void put(ByteBuffer b)
                    throws IOException;

            /**
             * Must be written in {@link java.nio.ByteOrder.LITTLE_ENDIAN
             * LITTLE_ENDIAN} byte order
             */
            void putInt(int i)
                    throws IOException;

            /**
             * If any data has been logically written to the file, but not yet
             * persisted to the underlying storage media, force the data to be
             * written to the storage media.
             */
            void sync()
                    throws IOException;

            /**
             * Called when the writer is finished writing to this region
             */
            @Override
            void close()
                    throws IOException;
        }
    }

    /**
     * A file which performs all writes as sequential appends. Will only be used
     * by one thread at a time
     */
    public interface SequentialWriteFile
            extends WritableByteChannel
    {
        /**
         * If any data has been logically written to the file, but not yet
         * persisted to the underlying storage media, force the data to be
         * written to the storage media.
         */
        void sync()
                throws IOException;

        /**
         * @return The current size of this file, measured in bytes
         */
        long size()
                throws IOException;
    }

    /**
     * A temporary file which serves to atomically save data to a particular
     * file name
     */
    public interface TemporaryWriteFile
            extends SequentialWriteFile
    {
        /**
         * Atomically save the data written to the temporary file to the given
         * target file name. The temporary file will no longer be written to,
         * and may be closed as necessary
         */
        public void save()
                throws IOException;
    }

    /**
     * A file which is read sequentially. Will only be used by one thread at a
     * time
     */
    public interface SequentialReadFile
            extends ReadableByteChannel
    {
        /**
         * Skip {@code n} bytes in the file
         */
        void skip(long n)
                throws IOException;
    }

    /**
     * A file which is read randomly. May be used by many threads concurrently.
     */
    public interface RandomReadFile
            extends Channel
    {
        /**
         * Reads a sequence of bytes from this file into a buffer starting at
         * the given file position.
         *
         *
         * @param position
         *            The file position at which the transfer read is to begin;
         *            be non-negative
         *
         * @param length
         *            the number of bytes to read from the file; must be
         *            non-negative
         *
         * @return a buffer containing the bytes read, possibly zero, or
         *         <tt>null</tt> if the given position is greater than or equal
         *         to the file's current size
         */
        ByteBuffer read(long position, int length)
                throws IOException;

        /**
         * @return the {@link Deallocator} with which to free buffers returned
         *         by this class
         */
        Deallocator deallocator();

        /**
         * @return The current size of this file, measured in bytes
         */
        long size()
                throws IOException;
    }

    /**
     * An implementation of Env that delegates all calls to another Env. May be
     * useful to clients who wish to override just part of the functionality of
     * another Env.
     */
    public static abstract class DelegateEnv
            implements Env
    {
        protected final Env delegate;

        public DelegateEnv(Env delegate)
        {
            this.delegate = delegate;
        }

        public ConcurrentWriteFile openMultiWriteFile(FileInfo info)
                throws IOException
        {
            return delegate.openMultiWriteFile(info);
        }

        public SequentialWriteFile openSequentialWriteFile(FileInfo info)
                throws IOException
        {
            return delegate.openSequentialWriteFile(info);
        }

        public TemporaryWriteFile openTemporaryWriteFile(FileInfo temp, FileInfo target)
                throws IOException
        {
            return delegate.openTemporaryWriteFile(temp, target);
        }

        public SequentialReadFile openSequentialReadFile(FileInfo info)
                throws IOException
        {
            return delegate.openSequentialReadFile(info);
        }

        public RandomReadFile openRandomReadFile(FileInfo info)
                throws IOException
        {
            return delegate.openRandomReadFile(info);
        }

        public void deleteFile(FileInfo info)
                throws IOException
        {
            delegate.deleteFile(info);
        }

        public boolean fileExists(FileInfo info)
                throws IOException
        {
            return delegate.fileExists(info);
        }

        public DBHandle createDBDir(DBHandle existing)
                throws IOException
        {
            return delegate.createDBDir(existing);
        }

        public void deleteDir(DBHandle handle)
                throws IOException
        {
            delegate.deleteDir(handle);
        }

        public Iterable<FileInfo> getOwnedFiles(DBHandle handle)
                throws IOException
        {
            return delegate.getOwnedFiles(handle);
        }

        public LockFile lockFile(FileInfo info)
                throws IOException
        {
            return delegate.lockFile(info);
        }
    }

    /*
     * Dear Java,
     * 
     * Your lack of metaprogramming disappoints me
     * 
     * Dear Eclipse,
     * 
     * Thank you for your mechanical compassion
     */

    /**
     * @see {@link DelegateEnv}
     */
    public static abstract class DelegateConcurrentWriteFile
            implements ConcurrentWriteFile
    {
        protected final ConcurrentWriteFile delegate;

        public DelegateConcurrentWriteFile(ConcurrentWriteFile delegate)
        {
            this.delegate = delegate;
        }

        public void close()
                throws IOException
        {
            delegate.close();
        }

        public WriteRegion requestRegion(LongToIntFunction getSize)
                throws IOException
        {
            return delegate.requestRegion(getSize);
        }
    }

    /**
     * @see {@link DelegateEnv}
     */
    public static abstract class DelegateWriteRegion
            implements WriteRegion
    {
        protected final WriteRegion delegate;

        public DelegateWriteRegion(WriteRegion delegate)
        {
            this.delegate = delegate;
        }

        public long startPosition()
        {
            return delegate.startPosition();
        }

        public void put(byte b)
                throws IOException
        {
            delegate.put(b);
        }

        public void put(ByteBuffer b)
                throws IOException
        {
            delegate.put(b);
        }

        public void putInt(int i)
                throws IOException
        {
            delegate.putInt(i);
        }

        public void sync()
                throws IOException
        {
            delegate.sync();
        }

        public void close()
                throws IOException
        {
            delegate.close();
        }
    }

    /**
     * @see {@link DelegateEnv}
     */
    public static abstract class DelegateSequentialWriteFile
            implements SequentialWriteFile
    {
        protected final SequentialWriteFile delegate;

        public DelegateSequentialWriteFile(SequentialWriteFile delegate)
        {
            this.delegate = delegate;
        }

        public int write(ByteBuffer src)
                throws IOException
        {
            return delegate.write(src);
        }

        public boolean isOpen()
        {
            return delegate.isOpen();
        }

        public void close()
                throws IOException
        {
            delegate.close();
        }

        public void sync()
                throws IOException
        {
            delegate.sync();
        }

        public long size()
                throws IOException
        {
            return delegate.size();
        }
    }

    /**
     * @see {@link DelegateEnv}
     */
    public static abstract class DelegateSequentialReadFile
            implements SequentialReadFile
    {
        protected final SequentialReadFile delegate;

        public DelegateSequentialReadFile(SequentialReadFile delegate)
        {
            this.delegate = delegate;
        }

        public int read(ByteBuffer dst)
                throws IOException
        {
            return delegate.read(dst);
        }

        public boolean isOpen()
        {
            return delegate.isOpen();
        }

        public void close()
                throws IOException
        {
            delegate.close();
        }

        public void skip(long n)
                throws IOException
        {
            delegate.skip(n);
        }
    }

    /**
     * @see {@link DelegateEnv}
     */
    public static abstract class DelegateRandomReadFile
            implements RandomReadFile
    {
        protected final RandomReadFile delegate;

        public DelegateRandomReadFile(RandomReadFile delegate)
        {
            this.delegate = delegate;
        }

        public boolean isOpen()
        {
            return delegate.isOpen();
        }

        public void close()
                throws IOException
        {
            delegate.close();
        }

        public ByteBuffer read(long position, int length)
                throws IOException
        {
            return delegate.read(position, length);
        }

        public Deallocator deallocator()
        {
            return delegate.deallocator();
        }

        public long size()
                throws IOException
        {
            return delegate.size();
        }
    }

    /**
     * @see {@link DelegateEnv}
     */
    public static abstract class DelegateTemporaryWriteFile
            implements TemporaryWriteFile
    {
        protected final TemporaryWriteFile delegate;

        public DelegateTemporaryWriteFile(TemporaryWriteFile delegate)
        {
            this.delegate = delegate;
        }

        public int write(ByteBuffer src)
                throws IOException
        {
            return delegate.write(src);
        }

        public boolean isOpen()
        {
            return delegate.isOpen();
        }

        public void sync()
                throws IOException
        {
            delegate.sync();
        }

        public long size()
                throws IOException
        {
            return delegate.size();
        }

        public void save()
                throws IOException
        {
            delegate.save();
        }

        public void close()
                throws IOException
        {
            delegate.close();
        }
    }

    /**
     * @see {@link DelegateEnv}
     */
    public static abstract class DelegateLockFile
            implements LockFile
    {
        protected final LockFile delegate;

        public DelegateLockFile(LockFile delegate)
        {
            this.delegate = delegate;
        }

        public boolean isValid()
        {
            return delegate.isValid();
        }

        public void close()
                throws IOException
        {
            delegate.close();
        }
    }
}