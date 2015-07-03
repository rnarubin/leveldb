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

/**
 * An Env is an interface used by the leveldb implementation to access operating
 * system functionality like the filesystem etc. Callers may wish to provide a
 * custom Env object when opening a database to get fine gain control; e.g., to
 * utilize SMR drives
 */
public interface Env
{

    /**
     * Creates and opens a new {@link MultiWriteFile} with the given name. If a
     * file already exists with this name, the existing file is first deleted.
     * 
     * @param name
     *            a plain string name identifying the file
     */
    MultiWriteFile openMultiWriteFile(String name)
            throws IOException;

    /**
     * Creates and opens a new {@link SequentialWriteFile} with the given name.
     * If a file already exists with this name, the existing file is first
     * deleted.
     * 
     * @param name
     *            a plain string name identifying the file
     */
    SequentialWriteFile openSequentialWriteFile(String name)
            throws IOException;

    /**
     * Opens an existing {@link SequentialReadFile} with the given name.
     * 
     * @param name
     *            a plain string name identifying the file
     * @throws FileNotFoundException
     *             if a file with this name does not exist
     */
    SequentialReadFile openSequentialReadFile(String name)
            throws IOException;

    /**
     * Opens an existing {@link RandomReadFile} with the given name.
     * 
     * @param name
     *            a plain string name identifying the file
     * @throws FileNotFoundException
     *             if a file with this name does not exist
     */
    RandomReadFile openRandomReadFile(String name)
            throws IOException;

    /**
     * Deletes the file with the given name
     * 
     * @throws FileNotFoundException
     *             if a file with this name does not exist
     */
    void deleteFile(String name)
            throws IOException;

    /**
     * @return true iff a file with the given name exists
     */
    boolean fileExists(String name);

    /**
     * A file to which multiple writers may attempt to append simultaneously.
     * Safe concurrency is facilitated by the space request mechanism: each
     * writer will provide a function which, given the current end position of
     * the file, will return the amount of space this writer will use. This
     * amount of space should be exclusively reserved for the writer beginning
     * at the given end position
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
    public interface MultiWriteFile
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
         * A region of the {@link MultiWriteFile} which has been exclusively
         * reserved by a single writer
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
}
