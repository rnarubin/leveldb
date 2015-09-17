/*
 * Copyright (C) 2011 the original author or authors. See the notice.md file distributed with this
 * work for additional information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.iq80.leveldb;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.LongToIntFunction;

import org.iq80.leveldb.Env.ConcurrentWriteFile.WriteRegion;

/**
 * An Env is an interface used by the leveldb implementation to access operating system
 * functionality like the filesystem etc. Callers may wish to provide a custom Env object when
 * opening a database to get fine gain control; e.g., to utilize SMR drives
 */
public interface Env {
  /**
   * Creates and opens a new {@link ConcurrentWriteFile} for the given file info. If a file already
   * exists with this info, the existing file is first deleted.
   */
  CompletionStage<? extends ConcurrentWriteFile> openConcurrentWriteFile(FileInfo info);

  /**
   * Creates and opens a new {@link SequentialWriteFile} with the given file info. If a file already
   * exists with this info, the existing file is first deleted.
   */
  CompletionStage<? extends SequentialWriteFile> openSequentialWriteFile(FileInfo info);

  /**
   * Creates and opens a new {@link TemporaryWriteFile} with the given temp file info. If a file
   * already exists with the temp info, the existing file is first deleted. Upon closing, the data
   * in the temp file should be atomically saved to the target file info, replacing the target file
   * if it exists
   */
  CompletionStage<? extends TemporaryWriteFile> openTemporaryWriteFile(FileInfo temp,
      FileInfo target);

  /**
   * Opens an existing {@link SequentialReadFile} with the given file info.
   *
   * @throws FileNotFoundException if a file with this info does not exist
   */
  CompletionStage<? extends SequentialReadFile> openSequentialReadFile(FileInfo info);

  /**
   * Opens an existing {@link RandomReadFile} with the given file info.
   *
   * @throws FileNotFoundException if a file with this info does not exist
   */
  CompletionStage<? extends RandomReadFile> openRandomReadFile(FileInfo info);

  /**
   * Lock the specified file. Used to prevent concurrent access to the same db by multiple
   * processes. The caller should call LockFile.close() to release the lock. If the process exits,
   * the lock should be automatically released.
   *
   * If somebody else already holds the lock, finishes immediately, i.e. this call does not wait for
   * existing locks to go away.
   *
   * May create the named file if it does not already exist.
   *
   * @return a LockFile object associated with the file. Calling {@link LockFile#isValid()} will
   *         indicate whether the file was successfully locked
   */
  CompletionStage<? extends LockFile> lockFile(FileInfo info);

  /**
   * @return true iff a file with the given file info exists; false iff the file does not exist.
   *         Otherwise, i.e. the file existence cannot be determined, throw an IOException
   *         describing the issue
   */
  CompletionStage<Boolean> fileExists(FileInfo info);

  /**
   * Deletes the file with the given file info
   *
   * @throws FileNotFoundException if a file with this info does not exist
   */
  CompletionStage<Void> deleteFile(FileInfo info);

  /**
   * Creates the necessary structures to contain all files owned by this database (e.g. creates a
   * directory in a filesystem) if the database does not already exist. Does nothing if it does
   * exist
   *
   * @param existing an optional handle to an already existing database. Can be empty, indicating
   *        the database does not yet exist or its existence has not been determined
   * @return a handle to the created database
   */
  CompletionStage<? extends DBHandle> createDB(Optional<DBHandle> existing);

  /**
   * Deletes the specified database and all files associated with it
   */
  CompletionStage<Void> deleteDB(final DBHandle handle);

  /**
   * Returns an {@link AsynchronousIterator} over the files owned by the given database
   */
  CompletionStage<? extends AsynchronousIterator<FileInfo>> getOwnedFiles(DBHandle handle);

  /**
   * Returns an executor which will be used by the application to run background and some
   * asynchronous work
   */
  Executor getExecutor();

  /**
   * A handle used to identify the set of files associated with a particular database
   */
  /*
   * the right thing to do here would actually be use a generic type parameter instead of this
   * handle. the trouble is that that type would have to be woven into DbImpl, Env, FileInfo,
   * Options, etc, at which point the gain of type safety in the already rare case of using a
   * non-default env is outweighed by code simplicity
   */
  public interface DBHandle {
  }

  public interface LockFile extends AsynchronousCloseable {
    /**
     * Indicates whether the file was successfully locked by this object
     */
    public boolean isValid();

    /**
     * Close the file, releasing its associated lock in the process
     */
    @Override
    public CompletionStage<Void> asyncClose();
  }

  /**
   * A file to which multiple writers may attempt to append concurrently. Safe concurrency is
   * facilitated by the space request mechanism: each writer will provide a function which, given
   * the next writable position of the file, will return the amount of space this writer will use.
   * This amount of space should be exclusively reserved for the writer beginning at the given
   * position
   * <p>
   * An example implementation may maintain an internal buffer, utilizing an atomically updated
   * offset within the buffer as the logical end of file. Multiple writers may then insert into the
   * buffer simultaneously. The buffer can be periodically flushed to the file at its physical end
   * position
   * </p>
   * <p>
   * Alternatively, a simple implementation may maintain a mutex which must be acquired by each
   * writer. This would allow each write to perform a conventional file append, although a backing
   * buffer for each {@link WriteRegion} is recommended as writers may submit small data fragments
   * at a time
   */
  public interface ConcurrentWriteFile extends AsynchronousCloseable {
    /**
     * Given the current position of the file, return a {@link WriteRegion} of the specified size
     * beginning at that position for use by a single writer
     *
     * @param getSize a function which applied to the current end position of the file returns the
     *        amount of space that the writer will use
     */
    CompletionStage<? extends WriteRegion> requestRegion(LongToIntFunction getSize);

    /**
     * A region of the {@link ConcurrentWriteFile} which has been exclusively reserved by a single
     * writer
     */
    public interface WriteRegion extends AsynchronousCloseable {
      /**
       * @return the position within the file at which this region begins
       */
      long startPosition();

      void put(byte b);

      void put(ByteBuffer b);

      /**
       * Must be written in {@link java.nio.ByteOrder#LITTLE_ENDIAN little-endian} byte order
       */
      void putInt(int i);

      /**
       * If any data has been logically written to the file, but not yet persisted to the underlying
       * storage media, force the data to be written to the storage media.
       */
      CompletionStage<Void> sync();

      /**
       * Called when the writer is finished writing to this region
       */
      @Override
      CompletionStage<Void> asyncClose();
    }
  }

  /**
   * A file which performs all writes as sequential appends. Will only be used by one thread at a
   * time
   */
  public interface SequentialWriteFile extends AsynchronousCloseable {

    /**
     * Appends the given data buffer to the file
     *
     * @return the position at which the data was written
     */
    CompletionStage<Long> write(ByteBuffer src);

    /**
     * If any data has been logically written to the file, but not yet persisted to the underlying
     * storage media, force the data to be written to the storage media.
     */
    CompletionStage<Void> sync();
  }

  /**
   * A temporary file which serves to atomically save data to a particular file name
   */
  public interface TemporaryWriteFile extends SequentialWriteFile {

    /**
     * Save the data written to the temporary file to the given target file name (in addition to
     * releasing associated resources as required by {@link Closeable#close() close})
     */
    @Override
    public CompletionStage<Void> asyncClose();
  }

  /**
   * A file which is read sequentially. Will only be used by one thread at a time
   */
  public interface SequentialReadFile extends AsynchronousCloseable {

    /**
     * Reads a sequence of bytes from this channel into the given buffer.
     */
    CompletionStage<Integer> read(ByteBuffer dst);

    /**
     * Skip {@code n} bytes in the file
     */
    CompletionStage<Void> skip(long n);
  }

  /**
   * A file which is read randomly. May be used by many threads concurrently.
   */
  public interface RandomReadFile extends AsynchronousCloseable {
    /**
     * Reads a sequence of bytes from this file into a buffer starting at the given file position.
     *
     *
     * @param position The file position at which the transfer read is to begin; be non-negative
     *
     * @param length the number of bytes to read from the file; must be non-negative
     *
     * @return a buffer containing the bytes read, possibly zero, or <tt>null</tt> if the given
     *         position is greater than or equal to the file's current size. The buffer must be in
     *         {@link java.nio.ByteOrder#LITTLE_ENDIAN little-endian} byte order
     */
    CompletionStage<ByteBuffer> read(long position, int length);

    /**
     * @return The current size of this file, measured in bytes
     */
    long size();
  }

  /**
   * An implementation of Env that delegates all calls to another Env. May be useful to clients who
   * wish to override just part of the functionality of another Env.
   */
  public static abstract class DelegateEnv implements Env {
    protected final Env delegate;

    public DelegateEnv(final Env delegate) {
      this.delegate = delegate;
    }

    @Override
    public CompletionStage<? extends ConcurrentWriteFile> openConcurrentWriteFile(
        final FileInfo info) {
      return delegate.openConcurrentWriteFile(info);
    }

    @Override
    public CompletionStage<? extends SequentialWriteFile> openSequentialWriteFile(
        final FileInfo info) {
      return delegate.openSequentialWriteFile(info);
    }

    @Override
    public CompletionStage<? extends TemporaryWriteFile> openTemporaryWriteFile(final FileInfo temp,
        final FileInfo target) {
      return delegate.openTemporaryWriteFile(temp, target);
    }

    @Override
    public CompletionStage<? extends SequentialReadFile> openSequentialReadFile(
        final FileInfo info) {
      return delegate.openSequentialReadFile(info);
    }

    @Override
    public CompletionStage<? extends RandomReadFile> openRandomReadFile(final FileInfo info) {
      return delegate.openRandomReadFile(info);
    }

    @Override
    public CompletionStage<Void> deleteFile(final FileInfo info) {
      return delegate.deleteFile(info);
    }

    @Override
    public CompletionStage<Boolean> fileExists(final FileInfo info) {
      return delegate.fileExists(info);
    }

    @Override
    public CompletionStage<? extends DBHandle> createDB(final Optional<DBHandle> existing) {
      return delegate.createDB(existing);
    }

    @Override
    public CompletionStage<Void> deleteDB(final DBHandle handle) {
      return delegate.deleteDB(handle);
    }

    @Override
    public CompletionStage<? extends AsynchronousIterator<FileInfo>> getOwnedFiles(
        final DBHandle handle) {
      return delegate.getOwnedFiles(handle);
    }

    @Override
    public CompletionStage<? extends LockFile> lockFile(final FileInfo info) {
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
  public static abstract class DelegateConcurrentWriteFile implements ConcurrentWriteFile {
    protected final ConcurrentWriteFile delegate;

    public DelegateConcurrentWriteFile(final ConcurrentWriteFile delegate) {
      this.delegate = delegate;
    }

    @Override
    public CompletionStage<Void> asyncClose() {
      return delegate.asyncClose();
    }

    @Override
    public CompletionStage<? extends WriteRegion> requestRegion(final LongToIntFunction getSize) {
      return delegate.requestRegion(getSize);
    }
  }

  /**
   * @see {@link DelegateEnv}
   */
  public static abstract class DelegateWriteRegion implements WriteRegion {
    protected final WriteRegion delegate;

    public DelegateWriteRegion(final WriteRegion delegate) {
      this.delegate = delegate;
    }

    @Override
    public long startPosition() {
      return delegate.startPosition();
    }

    @Override
    public void put(final byte b) {
      delegate.put(b);
    }

    @Override
    public void put(final ByteBuffer b) {
      delegate.put(b);
    }

    @Override
    public void putInt(final int i) {
      delegate.putInt(i);
    }

    @Override
    public CompletionStage<Void> sync() {
      return delegate.sync();
    }

    @Override
    public CompletionStage<Void> asyncClose() {
      return delegate.asyncClose();
    }
  }

  /**
   * @see {@link DelegateEnv}
   */
  public static abstract class DelegateSequentialWriteFile implements SequentialWriteFile {
    protected final SequentialWriteFile delegate;

    public DelegateSequentialWriteFile(final SequentialWriteFile delegate) {
      this.delegate = delegate;
    }

    @Override
    public CompletionStage<Void> asyncClose() {
      return delegate.asyncClose();
    }

    @Override
    public CompletionStage<Long> write(final ByteBuffer src) {
      return delegate.write(src);
    }

    @Override
    public CompletionStage<Void> sync() {
      return delegate.sync();
    }
  }

  /**
   * @see {@link DelegateEnv}
   */
  public static abstract class DelegateSequentialReadFile implements SequentialReadFile {
    protected final SequentialReadFile delegate;

    public DelegateSequentialReadFile(final SequentialReadFile delegate) {
      this.delegate = delegate;
    }

    @Override
    public CompletionStage<Void> asyncClose() {
      return delegate.asyncClose();
    }

    @Override
    public CompletionStage<Integer> read(final ByteBuffer dst) {
      return delegate.read(dst);
    }

    @Override
    public CompletionStage<Void> skip(final long n) {
      return delegate.skip(n);
    }
  }

  /**
   * @see {@link DelegateEnv}
   */
  public static abstract class DelegateRandomReadFile implements RandomReadFile {
    protected final RandomReadFile delegate;

    public DelegateRandomReadFile(final RandomReadFile delegate) {
      this.delegate = delegate;
    }

    @Override
    public CompletionStage<Void> asyncClose() {
      return delegate.asyncClose();
    }

    @Override
    public CompletionStage<ByteBuffer> read(final long position, final int length) {
      return delegate.read(position, length);
    }

    @Override
    public long size() {
      return delegate.size();
    }
  }

  /**
   * @see {@link DelegateEnv}
   */
  public static abstract class DelegateTemporaryWriteFile implements TemporaryWriteFile {
    protected final TemporaryWriteFile delegate;

    public DelegateTemporaryWriteFile(final TemporaryWriteFile delegate) {
      this.delegate = delegate;
    }

    @Override
    public CompletionStage<Long> write(final ByteBuffer src) {
      return delegate.write(src);
    }

    @Override
    public CompletionStage<Void> sync() {
      return delegate.sync();
    }

    @Override
    public CompletionStage<Void> asyncClose() {
      return delegate.asyncClose();
    }
  }

  /**
   * @see {@link DelegateEnv}
   */
  public static abstract class DelegateLockFile implements LockFile {
    protected final LockFile delegate;

    public DelegateLockFile(final LockFile delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean isValid() {
      return delegate.isValid();
    }

    @Override
    public CompletionStage<Void> asyncClose() {
      return delegate.asyncClose();
    }
  }
}
