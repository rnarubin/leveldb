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

package com.cleversafe.leveldb.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ManagedBlocker;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongToIntFunction;
import java.util.function.Predicate;

import com.cleversafe.leveldb.AsynchronousCloseable;
import com.cleversafe.leveldb.FileInfo;
import com.cleversafe.leveldb.util.ByteBuffers;
import com.cleversafe.leveldb.util.Closeables;
import com.cleversafe.leveldb.util.CompletableFutures;
import com.cleversafe.leveldb.util.MemoryManagers;
import com.cleversafe.leveldb.util.ObjectPool.PooledObject;
import com.cleversafe.leveldb.util.ObjectPools;
import com.cleversafe.leveldb.util.ObjectPools.DirectBufferPool;
import com.google.common.base.Throwables;

public class FileEnv extends PathEnv {

  public FileEnv() {
    this(false);
  }

  public FileEnv(final boolean legacySST) {
    super(legacySST);
  }

  @Override
  protected CompletionStage<? extends ConcurrentWriteFile> openConcurrentWriteFile(
      final Path path) {
    return submit(() -> new ConcurrentWriteFileImpl(path));
  }

  @Override
  protected CompletionStage<? extends SequentialWriteFile> openSequentialWriteFile(
      final Path path) {
    return submit(() -> new SequentialWriteFileImpl(path));
  }

  @Override
  protected CompletionStage<? extends TemporaryWriteFile> openTemporaryWriteFile(final Path temp,
      final Path target) {
    return submit(() -> new TemporaryWriteFileImpl(temp, target));
  }

  @Override
  protected CompletionStage<? extends SequentialReadFile> openSequentialReadFile(final Path path) {
    return submit(() -> new SequentialReadFileImpl(path));
  }

  @Override
  protected CompletionStage<? extends RandomReadFile> openRandomReadFile(final Path path) {
    return submit(() -> new RandomReadFileImpl(path));
  }

  @Override
  protected CompletionStage<? extends LockFile> lockFile(final Path path) {
    return submit(() -> new LockFileImpl(path));
  }

  @Override
  protected CompletionStage<Void> deleteFile(final Path path) {
    return submitVoid(() -> Files.delete(path));
  }

  @Override
  protected CompletionStage<Boolean> fileExists(final Path path) {
    return submit(() -> Files.exists(path));
  }

  @Override
  protected CompletionStage<Void> createDirIfNecessary(final Path path) {
    return fileExists(path).thenCompose(exists -> exists
        ? submit(() -> Files.isDirectory(path))
            .thenCompose(isDir -> isDir ? CompletableFuture.completedFuture(null)
                : CompletableFutures.exceptionalFuture(new IllegalArgumentException(
                    "Database directory " + path + " is not a directory")))
        : submitVoid(() -> Files.createDirectories(path)));
  }

  @Override
  protected CompletionStage<Void> deleteDB(final Path path, final Predicate<Path> fileNameFilter) {
    // instead of blindly deleting the entire directory, delete all files owned by the DB; if the
    // dir is empty after those deletions, delete the dir
    // TODO(maybe) deletion default on interface, recurse with Y combinator
    return submit(() -> new DirectoryIterator<Path>(path, fileNameFilter, Function.identity()))
        .thenCompose(iter -> CompletableFutures.composeUnconditionally(
            CompletableFutures.flatMapIterator(iter, this::deleteFile, getExecutor()).thenCompose(
                deletions -> CompletableFutures.allOf(deletions).thenApply(voided -> null)),
            voided -> iter.asyncClose()))
        .thenCompose(voided -> submit(() -> !Files.list(path).findAny().isPresent())).thenCompose(
            isEmpty -> isEmpty ? deleteFile(path) : CompletableFuture.completedFuture(null));
  }

  @Override
  protected CompletionStage<DirectoryIterator<FileInfo>> getOwnedFiles(final Path dir,
      final Predicate<Path> fileNameFilter, final Function<Path, FileInfo> parser) {
    return submit(() -> new DirectoryIterator<FileInfo>(dir, fileNameFilter, parser));
  }

  @Override
  public Executor getExecutor() {
    return ForkJoinPool.commonPool();
  }

  private CompletionStage<Void> submitVoid(final ExceptionalRunnable run) {
    return submit(() -> {
      run.run();
      return null;
    });
  }

  private <T> CompletionStage<T> submit(final Callable<T> call) {
    final CompletableFuture<T> future = new CompletableFuture<>();
    getExecutor().execute(new BlockingCall<>(future, call));
    return future;
  }

  @FunctionalInterface
  private interface ExceptionalRunnable {
    void run() throws Exception;
  }

  private final static class BlockingCall<T> implements Runnable {
    private final ManagedBlocker blocker;

    public BlockingCall(final CompletableFuture<T> future, final Callable<T> call) {
      this.blocker = new ManagedBlocker() {
        private boolean isDone = false;

        @Override
        public boolean block() {
          try {
            future.complete(call.call());
          } catch (final Exception e) {
            future.completeExceptionally(e);
          }
          return isDone = true;
        }

        @Override
        public boolean isReleasable() {
          return isDone;
        }
      };
    }

    @Override
    public void run() {
      try {
        ForkJoinPool.managedBlock(blocker);
      } catch (final InterruptedException e) {
        Throwables.propagate(e);
      }
    }
  }

  private final class DirectoryIterator<T> implements AsynchronousCloseableIterator<T> {

    private final DirectoryStream<Path> dirStream;
    private final Iterator<Path> iter;
    private final Function<Path, T> parser;

    private DirectoryIterator(final Path dir, final Predicate<Path> fileNameFilter,
        final Function<Path, T> parser) throws IOException {
      this.dirStream = Files.newDirectoryStream(dir, fileNameFilter::test);
      this.iter = dirStream.iterator();
      this.parser = parser;
    }

    @Override
    public CompletionStage<Optional<T>> next() {
      return submit(() -> {
        try {
          return iter.hasNext() ? Optional.of(parser.apply(iter.next())) : Optional.empty();
        } catch (final DirectoryIteratorException e) {
          throw e.getCause();
        }
      });
    }

    @Override
    public CompletionStage<Void> asyncClose() {
      return submitVoid(() -> dirStream.close());
    }
  }

  private abstract class AsyncFile implements AsynchronousCloseable {
    protected final AsynchronousFileChannel channel;
    protected final Path path;

    protected AsyncFile(final Path path, final OpenOption... options) throws IOException {
      this.channel = AsynchronousFileChannel.open(path, options);
      this.path = path;
    }

    @Override
    public CompletionStage<Void> asyncClose() {
      return submitVoid(() -> channel.close());
    }

    @Override
    public String toString() {
      final String name = getClass().getSimpleName();
      return (name.isEmpty() ? getClass().getName() : name) + "[path=" + path + "]";
    }
  }

  private final class SequentialWriteFileImpl extends AsyncFile implements SequentialWriteFile {
    private final AtomicLong position = new AtomicLong(0);

    public SequentialWriteFileImpl(final Path path) throws IOException {
      super(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
    }

    @Override
    public CompletionStage<Long> write(final ByteBuffer src) {
      final CompletableFuture<Long> f = new CompletableFuture<>();
      final long pos = position.getAndAdd(src.remaining());
      channel.write(src, pos, null, new CompletionHandler<Integer, Void>() {
        @Override
        public void completed(final Integer result, final Void attachment) {
          getExecutor().execute(() -> f.complete(pos));
        }

        @Override
        public void failed(final Throwable exc, final Void attachment) {
          getExecutor().execute(() -> f.completeExceptionally(exc));
        }
      });
      return f;
    }

    @Override
    public CompletionStage<Void> sync() {
      return submitVoid(() -> channel.force(false));
    }
  }

  private final class RandomReadFileImpl extends AsyncFile implements RandomReadFile {
    private final long fileSize;

    public RandomReadFileImpl(final Path path) throws IOException {
      super(path, StandardOpenOption.READ);
      this.fileSize = channel.size();
    }

    @Override
    public CompletionStage<ByteBuffer> read(final long position, final int length) {
      final CompletableFuture<ByteBuffer> f = new CompletableFuture<>();
      final ByteBuffer dst = ByteBuffer.allocateDirect(length).order(ByteOrder.LITTLE_ENDIAN);
      channel.read(dst, position, null, new CompletionHandler<Integer, Void>() {
        @Override
        public void completed(final Integer length, final Void attachment) {
          getExecutor().execute(() -> {
            // TODO unify with readHandler maybe, probably requires dst reworking
            dst.flip();
            f.complete(dst);
          });
        }

        @Override
        public void failed(final Throwable exc, final Void attachment) {
          getExecutor().execute(() -> f.completeExceptionally(exc));
        }
      });
      return f;
    }

    @Override
    public long size() {
      return fileSize;
    }
  }

  private final CompletionHandler<Integer, CompletableFuture<Integer>> readHandler =
      new CompletionHandler<Integer, CompletableFuture<Integer>>() {
        @Override
        public void completed(final Integer result, final CompletableFuture<Integer> future) {
          getExecutor().execute(() -> future.complete(result));
        }

        @Override
        public void failed(final Throwable exc, final CompletableFuture<Integer> future) {
          getExecutor().execute(() -> future.completeExceptionally(exc));
        }
      };

  private final class SequentialReadFileImpl extends AsyncFile implements SequentialReadFile {
    private final AtomicLong position = new AtomicLong(0);

    public SequentialReadFileImpl(final Path path) throws IOException {
      super(path, StandardOpenOption.READ);
    }

    @Override
    public CompletionStage<Integer> read(final ByteBuffer dst) {
      final CompletableFuture<Integer> f = new CompletableFuture<>();
      channel.read(dst, position.getAndAdd(dst.remaining()), f, readHandler);
      return f;
    }

    @Override
    public CompletionStage<Void> skip(final long n) {
      position.addAndGet(n);
      return CompletableFuture.completedFuture(null);
    }
  }

  private final class TemporaryWriteFileImpl implements TemporaryWriteFile {
    private final SequentialWriteFileImpl tempFile;
    private final Path target;

    public TemporaryWriteFileImpl(final Path temp, final Path target) throws IOException {
      this.tempFile = new SequentialWriteFileImpl(temp);
      this.target = target;
    }

    @Override
    public CompletionStage<Long> write(final ByteBuffer src) {
      return tempFile.write(src);
    }

    @Override
    public CompletionStage<Void> sync() {
      return tempFile.sync();
    }

    @Override
    public CompletionStage<Void> asyncClose() {
      return tempFile.asyncClose().thenCompose(voided -> submitVoid(
          () -> Files.move(tempFile.path, target, StandardCopyOption.ATOMIC_MOVE)));
    }
  }

  private final class LockFileImpl extends AsyncFile implements LockFile {
    private final boolean isValid;

    public LockFileImpl(final Path path) throws IOException {
      super(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
      boolean succeeded;
      try {
        succeeded = channel.tryLock() != null;
      } catch (final OverlappingFileLockException e) {
        succeeded = false;
      }
      this.isValid = succeeded;
    }

    @Override
    public boolean isValid() {
      return isValid;
    }
  }

  private final CompletionHandler<Integer, CompletableFuture<Void>> regionWriteHandler =
      new CompletionHandler<Integer, CompletableFuture<Void>>() {
        @Override
        public void completed(final Integer result, final CompletableFuture<Void> future) {
          getExecutor().execute(() -> future.complete(null));
        }

        @Override
        public void failed(final Throwable exc, final CompletableFuture<Void> future) {
          getExecutor().execute(() -> future.completeExceptionally(exc));
        }
      };

  private static final int REGION_SCRATCH_SIZE = 4096;

  private final class ConcurrentWriteFileImpl extends AsyncFile implements ConcurrentWriteFile {
    private final AtomicLong filePosition = new AtomicLong(0);
    private final DirectBufferPool bufferPool =
        ObjectPools.directBufferPool(Runtime.getRuntime().availableProcessors() * 2,
            REGION_SCRATCH_SIZE, MemoryManagers.direct(), 4);

    public ConcurrentWriteFileImpl(final Path path) throws IOException {
      super(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    }

    @Override
    public CompletionStage<? extends WriteRegion> requestRegion(final LongToIntFunction getSize) {
      long regionPosition;
      int length;
      do {
        regionPosition = filePosition.get();
      } while (!filePosition.compareAndSet(regionPosition,
          regionPosition + (length = getSize.applyAsInt(regionPosition))));

      return CompletableFuture.completedFuture(new WriteRegionImpl(regionPosition, length));
    }

    @Override
    public CompletionStage<Void> asyncClose() {
      final CompletionStage<Void> f = super.asyncClose();
      bufferPool.close();
      return f;
    }

    private final class WriteRegionImpl implements WriteRegion {
      private final long regionPosition;
      private final ByteBuffer buffer;
      private final AutoCloseable cleanup;

      protected WriteRegionImpl(final long regionPosition, final int length) {
        this.regionPosition = regionPosition;
        if (length <= REGION_SCRATCH_SIZE) {
          final PooledObject<ByteBuffer> b = bufferPool.tryAcquire();
          this.buffer =
              b != null ? b.get() : ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN);
          this.cleanup = b;
        } else {
          this.buffer = ByteBuffer.allocateDirect(length).order(ByteOrder.LITTLE_ENDIAN);
          this.cleanup = () -> ByteBuffers.freeDirect(buffer);
        }
      }

      @Override
      public final long startPosition() {
        return regionPosition;
      }

      @Override
      public void put(final byte b) {
        buffer.put(b);
      }

      @Override
      public void putInt(final int b) {
        buffer.putInt(b);
      }

      @Override
      public void put(final ByteBuffer b) {
        buffer.put(b);
      }

      private CompletionStage<Void> flush() {
        return buffer.flip().hasRemaining() ? asyncWrite()
            : CompletableFuture.completedFuture(null);
      }

      private CompletionStage<Void> asyncWrite() {
        final CompletableFuture<Void> f = new CompletableFuture<>();
        channel.write(buffer, regionPosition, f, regionWriteHandler);
        return f;
      }

      @Override
      public final CompletionStage<Void> sync() {
        return flush().thenCompose(voided -> submitVoid(() -> channel.force(false)));
      }

      @Override
      public final CompletionStage<Void> asyncClose() {
        return flush().whenComplete((flushed, exception) -> Closeables.closeQuietly(cleanup));
      }
    }
  }
}
