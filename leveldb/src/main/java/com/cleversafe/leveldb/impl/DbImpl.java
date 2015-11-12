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

import static com.cleversafe.leveldb.impl.DbConstants.NUM_LEVELS;
import static com.cleversafe.leveldb.impl.SequenceNumber.MAX_SEQUENCE_NUMBER;
import static com.cleversafe.leveldb.impl.ValueType.DELETION;
import static com.cleversafe.leveldb.impl.ValueType.VALUE;
import static com.cleversafe.leveldb.util.CompletableFutures.closeAfter;
import static com.cleversafe.leveldb.util.CompletableFutures.filterMapAndCollapse;
import static com.cleversafe.leveldb.util.SizeOf.SIZE_OF_INT;
import static com.cleversafe.leveldb.util.SizeOf.SIZE_OF_LONG;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cleversafe.leveldb.DB;
import com.cleversafe.leveldb.DBIterator;
import com.cleversafe.leveldb.Env;
import com.cleversafe.leveldb.Env.DBHandle;
import com.cleversafe.leveldb.Env.LockFile;
import com.cleversafe.leveldb.Env.SequentialWriteFile;
import com.cleversafe.leveldb.FileInfo;
import com.cleversafe.leveldb.FileInfo.FileType;
import com.cleversafe.leveldb.Options;
import com.cleversafe.leveldb.ReadOptions;
import com.cleversafe.leveldb.Snapshot;
import com.cleversafe.leveldb.WriteBatch;
import com.cleversafe.leveldb.WriteOptions;
import com.cleversafe.leveldb.impl.Snapshots.SnapshotImpl;
import com.cleversafe.leveldb.impl.WriteBatchImpl.Handler;
import com.cleversafe.leveldb.table.Table;
import com.cleversafe.leveldb.table.TableBuilder;
import com.cleversafe.leveldb.util.ByteBuffers;
import com.cleversafe.leveldb.util.Closeables;
import com.cleversafe.leveldb.util.CompletableFutures;
import com.cleversafe.leveldb.util.DeletionQueue;
import com.cleversafe.leveldb.util.DeletionQueue.DeletionHandle;
import com.cleversafe.leveldb.util.InternalIterator;
import com.cleversafe.leveldb.util.MergingIterator;
import com.google.common.base.Preconditions;

public class DbImpl implements DB {

  private static final Logger LOGGER = LoggerFactory.getLogger(DbImpl.class);

  private static final ReadOptions DEFAULT_READ_OPTIONS = ReadOptions.make();
  private static final WriteOptions DEFAULT_WRITE_OPTIONS = WriteOptions.make();

  private final Options options;
  private final DBHandle dbHandle;
  private final LockFile dbLock;
  private final VersionSet versions;
  private final TableCache tableCache;
  private final InternalKeyComparator internalKeyComparator;
  private final ExceptionHandler exceptionHandler;
  private final AtomicReference<CompletionStage<Void>> bgCompaction;
  private final MemTables memTables;

  private final Snapshots snapshots = new Snapshots();
  private final AtomicBoolean shuttingDown = new AtomicBoolean();
  private final Collection<Long> pendingOutputs = new ConcurrentLinkedQueue<>();

  private final DeletionQueue<CompletionStage<?>> openOperations = new DeletionQueue<>();

  DbImpl(final Options options, final DBHandle dbHandle, final LockFile dbLock,
      final VersionSet versions, final TableCache tableCache,
      final InternalKeyComparator internalKeyComparator, final ExceptionHandler exceptionHandler,
      final MemTableAndLog memlog) {
    this.options = options;
    this.dbHandle = dbHandle;
    this.dbLock = dbLock;
    this.versions = versions;
    this.tableCache = tableCache;
    this.internalKeyComparator = internalKeyComparator;
    this.exceptionHandler = exceptionHandler;
    this.memTables = new MemTables(memlog);
    this.bgCompaction = new AtomicReference<>(versions.needsCompaction() ? scheduleCompaction()
        : CompletableFuture.completedFuture(null));
  }

  @Override
  public String toString() {
    return "DbImpl [" + dbHandle + "]";
  }

  public static CompletionStage<DbImpl> newDbImpl(final Options options,
      final DBHandle userHandle) {
    Preconditions.checkNotNull(options, "options is null");
    final Env env = options.env();

    final InternalKeyComparator internalKeyComparator =
        new InternalKeyComparator(options.comparator());

    // create the database dir if it does not already exist
    return env.createDB(Optional.ofNullable(userHandle)).thenCompose(dbHandle -> {
      final ExceptionHandler exceptionHandler = new ExceptionHandler(dbHandle);
      // Reserve ten files or so for other uses and give the rest to TableCache
      final int tableCacheSize = options.maxOpenFiles() - 10;
      final TableCache tableCache = new TableCache(dbHandle, tableCacheSize, internalKeyComparator,
          env, options.verifyChecksums(), options.compression(), exceptionHandler);

      final FileInfo lockFile = FileInfo.lock(dbHandle);
      return env.lockFile(lockFile).thenCompose(dbLock -> {
        if (!dbLock.isValid()) {
          throw new IllegalStateException("Unable to acquire lock on " + lockFile);
        }

        return env.fileExists(FileInfo.current(dbHandle)).thenCompose(currentExists -> {
          if (currentExists) {
            Preconditions.checkArgument(!options.errorIfExists(),
                "Database '%s' exists and the error if exists option is enabled", dbHandle);
          } else {
            Preconditions.checkArgument(options.createIfMissing(),
                "Database '%s' does not exist and the create if missing option is disabled",
                dbHandle);
          }

          return VersionSet.newVersionSet(dbHandle, tableCache, internalKeyComparator, env)
              .thenCompose(versionSet -> {

            // Recover from all newer log files than the ones named in the
            // descriptor (new log files may have been added by the previous
            // incarnation without registering them in the descriptor).
            //
            // Note that PrevLogNumber() is no longer used, but we pay
            // attention to it in case we are recovering a database
            // produced by an older version of leveldb.
            final long minLogNumber = versionSet.getLogNumber();
            final long previousLogNumber = versionSet.getPrevLogNumber();

            final VersionEdit edit = new VersionEdit();

            return env
                .getOwnedFiles(
                    dbHandle)
                .thenCompose(
                    fileIter -> closeAfter(
                        filterMapAndCollapse(fileIter,
                            fileInfo -> fileInfo.getFileType() == FileInfo.FileType.LOG
                                && ((fileInfo.getFileNumber() >= minLogNumber)
                                    || (fileInfo.getFileNumber() == previousLogNumber)),
                            Function.identity(), env.getExecutor()),
                        fileIter))
                .thenCompose(logStream -> CompletableFutures
                    .mapSequential(
                        logStream.sorted(Comparator.comparingLong(FileInfo::getFileNumber)),
                        logFile -> recoverLogFile(logFile, edit, options, dbHandle,
                            () -> versionSet.getAndIncrementNextFileNumber(),
                            internalKeyComparator))
                    .thenAccept(recoveredSequences -> recoveredSequences
                        .max(Comparator.naturalOrder()).ifPresent(maxSequence -> {
              final long lastSequence = versionSet.getLastSequence();
              if (lastSequence < maxSequence) {
                versionSet.getAndAddLastSequence(maxSequence - lastSequence);
              }
            }))).thenCompose(voided -> Logs.createLogWriter(
                FileInfo.log(dbHandle, versionSet.getAndIncrementNextFileNumber()), env))
                .thenCompose(logWriter -> {
              edit.setLogNumber(logWriter.getFileNumber());
              return versionSet.logAndApply(edit)
                  .thenCompose(voided -> deleteObsoleteFiles(versionSet.getLiveFiles(), Stream.of(),
                      versionSet.getLogNumber(), versionSet.getPrevLogNumber(),
                      versionSet.getManifestFileNumber(), tableCache, env, dbHandle))
                  .thenApply(voided -> new DbImpl(options, dbHandle, dbLock, versionSet, tableCache,
                      internalKeyComparator, exceptionHandler,
                      new MemTableAndLog(new MemTable(internalKeyComparator), logWriter)));
            });
          });
        });
      });
    });
  }

  private static CompletionStage<Long> recoverLogFile(final FileInfo logFile,
      final VersionEdit edit, final Options options, final DBHandle dbHandle,
      final LongSupplier fileNumbers, final InternalKeyComparator internalKeyComparator) {
    assert logFile.getFileType() == FileType.LOG;

    LOGGER.info("{} recovering log #{}", dbHandle, logFile.getFileNumber());

    return LogReader
        .newLogReader(options.env(), logFile, LogMonitors.loggingMonitor(), true,
            0)
        .thenCompose(logReader -> closeAfter(recoverStep(logReader, edit, options, dbHandle, null,
            0, LogMonitors.loggingMonitor(), internalKeyComparator, fileNumbers), logReader));
  }

  private static CompletionStage<Long> recoverStep(final LogReader logReader,
      final VersionEdit edit, final Options options, final DBHandle dbHandle, final MemTable _table,
      final long maxSequence, final LogMonitor logMonitor,
      final InternalKeyComparator internalKeyComparator, final LongSupplier fileNumbers) {
    // TODO safe recursion
    return CompletableFutures.thenComposeExceptional(logReader.next(), optRecord -> {
      if (optRecord.isPresent()) {
        final ByteBuffer record = optRecord.get();
        if (record.remaining() < 12) {
          logMonitor.corruption(record.remaining(), "log record too small");
          return recoverStep(logReader, edit, options, dbHandle, _table, maxSequence, logMonitor,
              internalKeyComparator, fileNumbers);
        }

        final long sequenceBegin = record.getLong();
        final int updateSize = record.getInt();

        // read entries
        final WriteBatchImpl writeBatch = readWriteBatch(record, updateSize);

        // apply entries to memTable
        final MemTable table = _table != null ? _table : new MemTable(internalKeyComparator);
        table.getAndAddApproximateMemoryUsage(writeBatch.getApproximateSize());
        writeBatch.forEach(new InsertIntoHandler(table, sequenceBegin));

        // update the maxSequence
        final long largerSequence = Math.max(sequenceBegin + updateSize - 1, maxSequence);

        // flush mem table if necessary
        if (table.approximateMemoryUsage() > options.writeBufferSize()) {
          assert !table.isEmpty();
          return buildTable(table.entryIterable(), fileNumbers.getAsLong(), internalKeyComparator,
              dbHandle, options).thenCompose(fileMetaData -> {
            if (fileMetaData.getFileSize() > 0) {
              edit.addFile(0, fileMetaData);
            }
            return recoverStep(logReader, edit, options, dbHandle, null, largerSequence, logMonitor,
                internalKeyComparator, fileNumbers);
          });
        } else {
          return recoverStep(logReader, edit, options, dbHandle, table, largerSequence, logMonitor,
              internalKeyComparator, fileNumbers);
        }

      } else {
        return CompletableFuture.completedFuture(maxSequence);
      }
    });
  }

  @Override
  public CompletionStage<Void> asyncClose() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void close() {
    if (shuttingDown.getAndSet(true)) {
      return;
    }

    backgroundCompaction.clear();

    try {
      compactionExecutor.shutdown();
      try {
        compactionExecutor.awaitTermination(1, TimeUnit.DAYS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      final MemTables tables = memTables;
      closeMemAndLog(tables.mutable, versions);

      if (tables.immutableExists()) {
        closeMemAndLog(tables.immutable, versions);
      }

      Closeables.closeQuietly(versions);
    } finally {
      try {
        Closeables.closeIO(tableCache, dbLock);
      } catch (final IOException e) {
        LOGGER.warn("{} error in closing", this, e);
      }
    }
  }

  private void closeMemAndLog(final MemTableAndLog memlog, final VersionSet versions) {
    memlog.acquireAll();
    try {
      try {
        if (memlog.isUnpersisted()) {
          final VersionEdit edit = new VersionEdit();
          writeLevel0Table(memlog.memTable, edit, versions.getCurrent());

          edit.setPreviousLogNumber(0);
          edit.setLogNumber(memlog.log.getFileNumber());
          versions.logAndApply(edit);
        }
      } finally {
        Closeables.closeIO(memlog.log, memlog.memTable);
      }
      if (memlog.isUnpersisted()) {
        // delete log as we've already flushed
        options.env().deleteFile(FileInfo.log(dbHandle, memlog.log.getFileNumber()));
      }
    } catch (final IOException e) {
      LOGGER.error("{} error in closing", this, e);
    } finally {
      memlog.releaseAll();
    }

  }

  @Override
  public String getProperty(final String name) {
    checkBackgroundException();
    return null;
  }

  private static CompletionStage<Void> deleteObsoleteFiles(final Stream<FileMetaData> liveFiles,
      final Stream<Long> pendingOutputs, final long logNumber, final long prevLogNumber,
      final long manifestNumber, final TableCache tableCache, final Env env,
      final DBHandle dbHandle) {
    // Make a set of all of the live files
    final Set<Long> live = Stream.concat(liveFiles.map(FileMetaData::getNumber), pendingOutputs)
        .collect(Collectors.toSet());

    final Predicate<FileInfo> shouldKeep = fileInfo -> {
      final long number = fileInfo.getFileNumber();
      switch (fileInfo.getFileType()) {
        case LOG:
          return ((number >= logNumber) || (number == prevLogNumber));
        case MANIFEST:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other
          // incarnations)
          return (number >= manifestNumber);
        case TABLE:
          return live.contains(number);
        case TEMP:
          // Any temp files that are currently being written to must be recorded in
          // pending_outputs_, which is inserted into "live"
          return live.contains(number);
        case INFO_LOG:
        case CURRENT:
        case DB_LOCK:
          return true;
        default:
          throw new IllegalArgumentException("unknown file type:" + fileInfo);
      }
    };

    return env.getOwnedFiles(dbHandle).thenCompose(fileIter -> closeAfter(
        filterMapAndCollapse(fileIter, shouldKeep.negate(), (final FileInfo fileInfo) -> {
          if (fileInfo.getFileType() == FileType.TABLE) {
            tableCache.evict(fileInfo.getFileNumber());
          }
          LOGGER.debug("{} delete type={} #{}", dbHandle, fileInfo.getFileType(),
              fileInfo.getFileNumber());
          return env.deleteFile(fileInfo);
        } , env.getExecutor()), fileIter))
        .thenCompose(deletionStream -> CompletableFutures.allOfVoid(deletionStream));
  }

  public void flushMemTable() {
    makeRoomForWrite(options.writeBufferSize()).release();

    // todo bg_error code
    // TODO manual flush not thread safe wrt close?
    MemTables tables;
    while ((tables = memTables).immutableExists()) {
      tables.waitForImmutableCompaction();
    }
    checkBackgroundException();
  }

  public void compactRange(final int level, final ByteBuffer start, final ByteBuffer end) {
    // TODO not thread safe
    Preconditions.checkArgument(level >= 0, "level is negative");
    Preconditions.checkArgument(level + 1 < NUM_LEVELS, "level is greater than or equal to %s",
        NUM_LEVELS);
    Preconditions.checkNotNull(start, "start is null");
    Preconditions.checkNotNull(end, "end is null");

    mutex.lock();
    try {
      while (this.manualCompaction != null) {
        backgroundCondition.awaitUninterruptibly();
      }
      final ManualCompaction manualCompaction = new ManualCompaction(level, start, end);
      this.manualCompaction = manualCompaction;

      old_scheduleCompaction();

      while (this.manualCompaction == manualCompaction) {
        backgroundCondition.awaitUninterruptibly();
      }
    } finally {
      mutex.unlock();
    }
  }

  private void maybeScheduleCompaction() {
    if (!shuttingDown.get() && (memTables.immutableExists() || manualCompaction != null
        || versions.needsCompaction())) {
      old_scheduleCompaction();
    }
  }

  private CompletionStage<Void> scheduleCompaction() {
    // FIXME
    return null;
  }

  private void old_scheduleCompaction() {
    if (backgroundCompaction.offer(new Runnable() {
      @Override
      public void run() {
        mutex.lock();
        try {
          if (!shuttingDown.get()) {
            backgroundCompaction();
          }
        } catch (final DatabaseShutdownException ignored) {
        } catch (final Throwable e) {
          setBackgroundException(e);
        } finally {
          try {
            maybeScheduleCompaction();
          } finally {
            backgroundCondition.signalAll();
            mutex.unlock();
          }
        }
      }
    })) {
      LOGGER.debug("{} scheduled compaction", this);
      /*
       * inserting into the blocking queue directly means that it is possible for a compaction to be
       * running in the pool while another is waiting in the queue (rather than the previous
       * implementation that limited to a total of 1 compaction either running or queued). This is a
       * compromise made to facilitate thread safety, in which compactions may be over-scheduled, in
       * favor of possibly missing a compaction of the immutable memtable
       */
    } else {
      LOGGER.trace("{} foregoing compaction, queue full", this);
    }
  }

  private void setBackgroundException(final Throwable t) {
    backgroundExceptionHandler.uncaughtException(Thread.currentThread(), t);
  }

  public void checkBackgroundException() {
    final Throwable e = backgroundException;
    if (e != null) {
      throw new BackgroundProcessingException(e);
    }
  }

  private void backgroundCompaction() throws IOException {
    LOGGER.debug("{} beginning compaction", this);

    compactMemTableInternal();

    Compaction compaction;
    if (manualCompaction != null) {
      compaction = versions.compactRange(manualCompaction.level,
          new TransientInternalKey(manualCompaction.begin, MAX_SEQUENCE_NUMBER, ValueType.VALUE),
          new TransientInternalKey(manualCompaction.end, 0L, ValueType.DELETION));
    } else {
      compaction = versions.pickCompaction();
    }

    if (compaction == null) {
      // no compaction
    } else if (manualCompaction == null && compaction.isTrivialMove()) {
      // Move file to next level
      Preconditions.checkState(compaction.getLevelInputs().size() == 1);
      final FileMetaData fileMetaData = compaction.getLevelInputs().get(0);
      compaction.getEdit().deleteFile(compaction.getLevel(), fileMetaData.getNumber());
      compaction.getEdit().addFile(compaction.getLevel() + 1, fileMetaData);
      versions.logAndApply(compaction.getEdit());
      // log
    } else {
      final CompactionState compactionState = new CompactionState(compaction);
      doCompactionWork(compactionState);
      cleanupCompaction(compactionState);
    }

    // manual compaction complete
    if (manualCompaction != null) {
      manualCompaction = null;
    }
  }

  private void cleanupCompaction(final CompactionState compactionState) throws IOException {
    if (compactionState.builder != null) {
      compactionState.builder.abandon();
      Closeables.closeIO(compactionState.builder, compactionState.outfile);
    } else {
      Preconditions.checkArgument(compactionState.outfile == null);
    }

    for (final FileMetaData output : compactionState.outputs) {
      pendingOutputs.remove(output.getNumber());
    }
  }


  @Override
  public CompletionStage<ByteBuffer> get(final ByteBuffer key) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CompletionStage<ByteBuffer> get(final ByteBuffer key, final ReadOptions options) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public byte[] get(final byte[] key) throws DBException {
    return get(key, DEFAULT_READ_OPTIONS);
  }

  @Override
  public byte[] get(final byte[] key, final ReadOptions readOptions) throws DBException {
    final LookupResult res = getInternal(ByteBuffer.wrap(key), readOptions);
    ByteBuffer b;
    if (res == null || (b = res.getValue()) == null) {
      return null;
    }
    final byte[] ret = ByteBuffers.toArray(ByteBuffers.duplicate(b));
    if (res.needsFreeing()) {
      options.memoryManager().free(b);
    }
    return ret;
  }

  @Override
  public ByteBuffer get(final ByteBuffer key) throws DBException {
    return get(key, DEFAULT_READ_OPTIONS);
  }

  @Override
  public ByteBuffer get(final ByteBuffer key, final ReadOptions readOptions) {
    return getInternal(key, readOptions).getValue();
  }

  public LookupResult getInternal(final ByteBuffer key, final ReadOptions readOptions)
      throws DBException {
    checkBackgroundException();
    LookupResult lookupResult;
    LookupKey lookupKey;
    final SnapshotImpl snapshot = getSnapshot(readOptions);
    lookupKey = new LookupKey(key, snapshot.getLastSequence());
    do {
      final MemTables tables = memTables;

      // First look in the memtable, then in the immutable memtable (if
      // any).
      {
        // no need to acquire memlog semaphore; closing memlog doesn't
        // matter to a get because we only care that the skiplist is
        // reachable
        final MemTable memtable = tables.mutable.memTable.retain();
        if (memtable == null) {
          // raced with memtable release, reacquire tables
          continue;
        }
        try {
          lookupResult = memtable.get(lookupKey);
          if (lookupResult != null) {
            return lookupResult;
          }
        } finally {
          memtable.release();
        }
      }
      if (tables.immutableExists()) {
        final MemTable immutable = tables.immutable.memTable.retain();
        if (immutable != null) {
          try {
            lookupResult = immutable.get(lookupKey);
            if (lookupResult != null) {
              return lookupResult;
            }
          } finally {
            immutable.release();
          }

        }
      }
      break;
    } while (true);

    // Not in memTables; try live files in level order
    try {
      lookupResult = versions.get(lookupKey);
    } catch (final IOException e) {
      throw new DBException(e);
    }

    // schedule compaction if necessary
    // TODO check mutex
    mutex.lock();
    try {
      if (versions.needsCompaction()) {
        old_scheduleCompaction();
      }
    } finally {
      mutex.unlock();
    }

    return lookupResult;
  }



  @Override
  // TODO measure cost of cast instead of wildcard
  public CompletionStage<?> put(final ByteBuffer key, final ByteBuffer value) {
    return put(key, value, DEFAULT_WRITE_OPTIONS);
  }

  @Override
  public CompletionStage<Snapshot> put(final ByteBuffer key, final ByteBuffer value,
      final WriteOptions writeOptions) {
    return writeInternal(new WriteBatchImpl.WriteBatchSingle(key, value), writeOptions);
  }

  @Override
  public CompletionStage<?> delete(final ByteBuffer key) {
    return delete(key, DEFAULT_WRITE_OPTIONS);
  }

  @Override
  public CompletionStage<Snapshot> delete(final ByteBuffer key, final WriteOptions writeOptions) {
    return writeInternal(new WriteBatchImpl.WriteBatchSingle(key), writeOptions);
  }

  @Override
  public CompletionStage<?> write(final WriteBatch updates) {
    return write(updates, DEFAULT_WRITE_OPTIONS);
  }

  @Override
  public CompletionStage<Snapshot> write(final WriteBatch updates,
      final WriteOptions writeOptions) {
    assert updates instanceof WriteBatchImpl;
    return writeInternal((WriteBatchImpl) updates, writeOptions);
  }

  private CompletionStage<Snapshot> writeInternal(final WriteBatchImpl updates,
      final WriteOptions writeOptions) {
    checkBackgroundException();

    final CompletableFuture<Snapshot> write = new CompletableFuture<>();
    final DeletionHandle pendingHandle = openOperations.insert(write);

    if (shuttingDown.get()) {
      write.complete(null);
      openOperations.delete(pendingHandle);
      return CompletableFutures.exceptionalFuture(
          new DatabaseShutdownException("attempted to write while database is shutting down"));
    }

    if (updates.size() == 0) {
      openOperations.delete(pendingHandle);
      write.complete(writeOptions.snapshot() ? getSnapshot() : null);
    } else {
      // TODO throttleWritesIfNecessary();

      class Box {
        MemTable memTable;
        long sequenceBegin;
      }
      final Box box = new Box();
      memTables.makeRoomForWrite(updates.getApproximateSize()).thenCompose(memlog -> {
        box.memTable = memlog.memTable;
        box.sequenceBegin = versions.getAndAddLastSequence(updates.size()) + 1;
        if (writeOptions.disableLog()) {
          memlog.markUnpersisted();
          return CompletableFuture.completedFuture(null);
        } else {
          return memlog.log.addRecord(writeWriteBatch(updates, box.sequenceBegin),
              writeOptions.sync());
        }
      }).whenComplete((successVoid, exception) -> {
        if (exception == null) {
          updates.forEach(new InsertIntoHandler(box.memTable, box.sequenceBegin));
          openOperations.delete(pendingHandle);
          write.complete(writeOptions.snapshot()
              ? snapshots.newSnapshot(box.sequenceBegin + updates.size() - 1) : null);
        } else {
          openOperations.delete(pendingHandle);
          write.completeExceptionally(exception);
        }
      });
    }
    return write;
  }

  /*
   * private void throttleWritesIfNecessary() { if (!this.options.throttleLevel0()) { return; }
   * boolean allowDelay = true; while (true) { if (allowDelay && versions.numberOfFilesInLevel(0) >
   * L0_SLOWDOWN_WRITES_TRIGGER) { // We are getting close to hitting a hard limit on the number of
   * // L0 files. Rather than delaying a single write by several // seconds when we hit the hard
   * limit, start delaying each // individual write by 1ms to reduce latency variance. Also, // this
   * delay hands over some CPU to the compaction thread in // case it is sharing the same core as
   * the writer. try { Thread.sleep(1); } catch (final InterruptedException e) {
   * Thread.currentThread().interrupt(); throw new RuntimeException(e); } allowDelay = false; } if
   * (versions.numberOfFilesInLevel(0) >= L0_STOP_WRITES_TRIGGER) { try { mutex.lock();
   * backgroundCondition.awaitUninterruptibly(); } finally { mutex.unlock(); } } else { break; } } }
   */

  @Override
  public WriteBatch createWriteBatch() {
    checkBackgroundException();
    return new WriteBatchImpl.WriteBatchMulti();
  }

  @Override
  public SeekingIteratorAdapter iterator() {
    final SeekingIteratorAdapter iter = iterator(DEFAULT_READ_OPTIONS);
    iter.seekToFirst();
    return iter;
  }

  @Override
  public SeekingIteratorAdapter iterator(final ReadOptions readOptions) {
    checkBackgroundException();
    mutex.lock();
    try {
      final MergingIterator rawIterator = internalIterator();

      // filter any entries not visible in our snapshot
      final SnapshotImpl snapshot = getSnapshot(readOptions);
      final SnapshotSeekingIterator snapshotIterator = new SnapshotSeekingIterator(rawIterator,
          snapshot, internalKeyComparator.getUserComparator());
      return new SeekingIteratorAdapter(snapshotIterator);
    } catch (final IOException e) {
      throw new DBException(e);
    } finally {
      mutex.unlock();
    }
  }

  MergingIterator internalIterator() throws IOException {
    // TODO check mutex
    mutex.lock();
    try {
      // merge together the memTable, immutableMemTable, and tables in version set
      final List<InternalIterator> iterators = new ArrayList<>();
      MemTable immutable = null, mutable;
      do {
        final MemTables tables = memTables;
        if (tables.immutableExists() && (immutable = tables.immutable.memTable.retain()) == null) {
          continue;
        }
        if ((mutable = tables.mutable.memTable.retain()) == null) {
          Closeables.closeIO(immutable);
          continue;
        }
        break;
      } while (true);

      try {
        if (immutable != null) {
          iterators.add(immutable.iterator());
        }
        iterators.add(mutable.iterator());
      } finally {
        Closeables.closeIO(mutable, immutable);
      }

      final Version current = versions.getCurrent();
      iterators.addAll(current.getLevel0Files());
      iterators.addAll(current.getLevelIterators());
      return new MergingIterator(iterators, internalKeyComparator);
    } finally {
      mutex.unlock();
    }
  }

  @Override
  public SnapshotImpl getSnapshot() {
    return snapshots.newSnapshot(versions.getLastSequence());
  }

  private SnapshotImpl getSnapshot(final ReadOptions options) {
    SnapshotImpl snapshot;
    if (options.snapshot() != null) {
      snapshot = (SnapshotImpl) options.snapshot();
    } else {
      // FIXME we do have to retain, wrap this in a conditional closeable maybe?
      snapshot = new SnapshotImpl(versions.getCurrent(), versions.getLastSequence());
      snapshot.close(); // To avoid holding the snapshot active..
    }
    return snapshot;
  }

  public void compactMemTable() throws IOException {
    compactMemTableInternal();
  }

  private void compactMemTableInternal() throws IOException {
    final MemTables tables = this.memTables;
    if (!tables.claimedForFlush.compareAndSet(false, true)) {
      return;
    }

    // Save the contents of the memtable as a new Table

    // block until pending writes have finished
    final MemTableAndLog immutable = tables.immutable.acquireAll();
    try {
      immutable.log.close();
      final VersionEdit edit = new VersionEdit();
      final Version base = versions.getCurrent();
      LOGGER.debug("{} flushing {}", this, immutable.memTable);
      writeLevel0Table(immutable.memTable, edit, base);

      if (shuttingDown.get()) {
        throw new DatabaseShutdownException("Database shutdown during memtable compaction");
      }

      edit.setPreviousLogNumber(0);
      edit.setLogNumber(immutable.log.getFileNumber()); // Earlier logs no longer needed
      versions.logAndApply(edit);

      this.memTables = new MemTables(this.memTables.mutable, null);
      immutable.memTable.release();
    } finally {
      try {
        tables.finishCompaction();
      } finally {
        immutable.releaseAll();
      }
    }

    deleteObsoleteFiles();
  }

  private CompletionStage<VersionEdit> writeLevel0Table(final MemTable mem) {
    // skip empty mem table
    if (mem.isEmpty()) {
      return CompletableFuture.completedFuture(new VersionEdit());
    }

    // write the memtable to a new sstable
    final long fileNumber = versions.getAndIncrementNextFileNumber();
    pendingOutputs.add(fileNumber);
    return buildTable(mem.entryIterable(), fileNumber, internalKeyComparator, dbHandle, options)
        .whenComplete((meta, exception) -> pendingOutputs.remove(fileNumber)).thenApply(meta -> {
          final VersionEdit edit = new VersionEdit();
          // Note that if file size is zero, the file has been deleted and
          // should not be added to the manifest.
          if (meta != null && meta.getFileSize() > 0) {
            final ByteBuffer minUserKey = meta.getSmallest().getUserKey();
            final ByteBuffer maxUserKey = meta.getLargest().getUserKey();
            final int level =
                // TODO harden this against concurrent compactions
                // base != null ? base.pickLevelForMemTableOutput(minUserKey, maxUserKey) :
                0;
            edit.addFile(level, meta);
          }
          return edit;
        });
  }

  private static CompletionStage<FileMetaData> buildTable(
      final Iterable<Entry<InternalKey, ByteBuffer>> data, final long fileNumber,
      final InternalKeyComparator internalKeyComparator, final DBHandle dbHandle,
      final Options options) {
    final Env env = options.env();
    final FileInfo fileInfo = FileInfo.table(dbHandle, fileNumber);
    return CompletableFutures.composeOnException(
        env.openSequentialWriteFile(fileInfo)
            .thenCompose(file -> closeAfter(buildTableStep(data.iterator(), fileNumber, file,
                options, internalKeyComparator, dbHandle), file)),
        buildFailedException -> env.fileExists(fileInfo).thenCompose(
            exists -> exists ? env.deleteFile(fileInfo) : CompletableFuture.completedFuture(null)));
  }

  private static final CompletionStage<FileMetaData> buildTableStep(
      final Iterator<Entry<InternalKey, ByteBuffer>> data, final long fileNumber,
      final SequentialWriteFile file, final Options options,
      final InternalKeyComparator internalKeyComparator, final DBHandle dbHandle) {
    Preconditions.checkArgument(data.hasNext(), "cannot build from empty table");

    final TableBuilder builder = new TableBuilder(options.blockRestartInterval(),
        options.blockSize(), options.compression(), file, internalKeyComparator);
    Entry<InternalKey, ByteBuffer> entry = data.next();
    final InternalKey smallest = entry.getKey();

    // TODO consider recursion if prohibitive for huge memtables
    CompletionStage<Void> chain = builder.add(entry.getKey(), entry.getValue());
    while (data.hasNext()) {
      entry = data.next();
      chain = builder.add(entry.getKey(), entry.getValue());
    }
    final InternalKey largest = entry.getKey();
    return chain.thenCompose(voided -> builder.finish())
        .thenCompose(fileSize -> TableCache
            // verify table can be opened
            .loadTable(options.env(), dbHandle, fileNumber, internalKeyComparator,
                options.verifyChecksums(), options.compression())
            .thenCompose(Table::release)
            .thenApply(voided -> new FileMetaData(fileNumber, fileSize, smallest, largest)));
  }


  private void doCompactionWork(final CompactionState compactionState) throws IOException {
    Preconditions.checkState(mutex.isHeldByCurrentThread());
    Preconditions.checkArgument(
        versions.numberOfBytesInLevel(compactionState.getCompaction().getLevel()) > 0);
    Preconditions.checkArgument(compactionState.builder == null);
    Preconditions.checkArgument(compactionState.outfile == null);

    // todo track snapshots
    compactionState.smallestSnapshot = versions.getLastSequence();

    // Release mutex while we're actually doing the compaction work
    mutex.unlock();
    try {
      try (MergingIterator iterator = versions.makeInputIterator(compactionState.compaction)) {

        ByteBuffer currentUserKey = null;
        boolean hasCurrentUserKey = false;

        long lastSequenceForKey = MAX_SEQUENCE_NUMBER;
        while (iterator.hasNext() && !shuttingDown.get()) {
          // always give priority to compacting the current mem table
          compactMemTableInternal();

          final InternalKey key = iterator.peek().getKey();
          if (compactionState.compaction.shouldStopBefore(key) && compactionState.builder != null) {
            finishCompactionOutputFile(compactionState);
          }

          // Handle key/value, add to state, etc.
          boolean drop = false;
          // todo if key doesn't parse (it is corrupted),
          /*
           * if (false //!ParseInternalKey(key, &ikey)) { // do not hide error keys currentUserKey =
           * null; hasCurrentUserKey = false; lastSequenceForKey = MAX_SEQUENCE_NUMBER; } else
           */
          {
            if (!hasCurrentUserKey || internalKeyComparator.getUserComparator()
                .compare(key.getUserKey(), currentUserKey) != 0) {
              // First occurrence of this user key
              currentUserKey = ByteBuffers.heapCopy(key.getUserKey());
              hasCurrentUserKey = true;
              lastSequenceForKey = MAX_SEQUENCE_NUMBER;
            }

            if (lastSequenceForKey <= compactionState.smallestSnapshot) {
              // Hidden by an newer entry for same user key
              drop = true; // (A)
            } else if (key.getValueType() == ValueType.DELETION
                && key.getSequenceNumber() <= compactionState.smallestSnapshot
                && compactionState.compaction.isBaseLevelForKey(key.getUserKey())) {

              // For this user key:
              // (1) there is no data in higher levels
              // (2) data in lower levels will have larger sequence numbers
              // (3) data in layers that are being compacted here and have
              // smaller sequence numbers will be dropped in the next
              // few iterations of this loop (by rule (A) above).
              // Therefore this deletion marker is obsolete and can be dropped.
              drop = true;
            }

            lastSequenceForKey = key.getSequenceNumber();
          }

          if (!drop) {
            // Open output file if necessary
            if (compactionState.builder == null) {
              openCompactionOutputFile(compactionState);
            }
            if (compactionState.builder.getEntryCount() == 0) {
              compactionState.currentSmallest = key.heapCopy();
            }
            compactionState.currentLargest = key;
            compactionState.builder.add(key, iterator.peek().getValue());

            // Close output file if it is big enough
            if (compactionState.builder.getFileSize() >= compactionState.compaction
                .getMaxOutputFileSize()) {
              compactionState.currentLargest = compactionState.currentLargest.heapCopy();
              finishCompactionOutputFile(compactionState);
            }
          }
          iterator.next();
        }
        if (compactionState.currentLargest != null) {
          compactionState.currentLargest = compactionState.currentLargest.heapCopy();
        }
      }

      if (shuttingDown.get()) {
        throw new DatabaseShutdownException("DB shutdown during compaction");
      }
      if (compactionState.builder != null) {
        finishCompactionOutputFile(compactionState);
      }
    } catch (final Throwable t) {
      try {
        cleanupCompaction(compactionState);
      } catch (final Throwable ignored) {
      }
      throw t;
    } finally {
      mutex.lock();
    }

    // todo port CompactionStats code

    installCompactionResults(compactionState);
  }

  private void openCompactionOutputFile(final CompactionState compactionState) throws IOException {
    Preconditions.checkNotNull(compactionState, "compactionState is null");
    Preconditions.checkArgument(compactionState.builder == null,
        "compactionState builder is not null");

    mutex.lock();
    try {
      final long fileNumber = versions.getNextFileNumber();
      pendingOutputs.add(fileNumber);
      compactionState.currentFileNumber = fileNumber;
      compactionState.currentFileSize = 0;
      compactionState.currentSmallest = null;
      compactionState.currentLargest = null;

      compactionState.outfile =
          options.env().openSequentialWriteFile(FileInfo.table(dbHandle, fileNumber));
      compactionState.builder =
          new TableBuilder(options, compactionState.outfile, internalKeyComparator);
    } finally {
      mutex.unlock();
    }
  }

  private void finishCompactionOutputFile(final CompactionState compactionState)
      throws IOException {
    Preconditions.checkNotNull(compactionState, "compactionState is null");
    Preconditions.checkArgument(compactionState.outfile != null);
    Preconditions.checkArgument(compactionState.builder != null);

    final long outputNumber = compactionState.currentFileNumber;
    Preconditions.checkArgument(outputNumber != 0);

    final long currentEntries = compactionState.builder.getEntryCount();
    compactionState.builder.finish();

    final long currentBytes = compactionState.builder.getFileSize();
    compactionState.currentFileSize = currentBytes;
    // compactionState.totalBytes += currentBytes;

    final FileMetaData currentFileMetaData =
        new FileMetaData(compactionState.currentFileNumber, compactionState.currentFileSize,
            compactionState.currentSmallest, compactionState.currentLargest);
    compactionState.outputs.add(currentFileMetaData);

    compactionState.builder.close();
    compactionState.builder = null;

    compactionState.outfile.sync();
    compactionState.outfile.close();
    compactionState.outfile = null;

    if (currentEntries > 0) {
      // Verify that the table is usable
      tableCache.newIterator(outputNumber).close();
    }
  }

  private void installCompactionResults(final CompactionState compact) {
    Preconditions.checkState(mutex.isHeldByCurrentThread());

    // Add compaction outputs
    compact.compaction.addInputDeletions(compact.compaction.getEdit());
    final int level = compact.compaction.getLevel();
    for (final FileMetaData output : compact.outputs) {
      compact.compaction.getEdit().addFile(level + 1, output);
      pendingOutputs.remove(output.getNumber());
    }

    try {
      versions.logAndApply(compact.compaction.getEdit());
      deleteObsoleteFiles();
    } catch (final IOException e) {
      LOGGER.debug("{} compaction failed due to {}. will try again later", this, e.toString());
      // Compaction failed for some reason. Simply discard the work and try again later.

      // Discard any files we may have created during this failed compaction
      for (final FileMetaData output : compact.outputs) {
        try {
          options.env().deleteFile(FileInfo.table(dbHandle, output.getNumber()));
        } catch (final IOException deleteFailed) {
          LOGGER.warn("{} failed to delete file {}", this, output, deleteFailed);
        }
      }
      compact.outputs.clear();
    }
  }

  int numberOfFilesInLevel(final int level) {
    return versions.getCurrent().numberOfFilesInLevel(level);
  }

  @Override
  public long[] getApproximateSizes(final Range... ranges) {
    Preconditions.checkNotNull(ranges, "ranges is null");
    final long[] sizes = new long[ranges.length];
    for (int i = 0; i < ranges.length; i++) {
      final Range range = ranges[i];
      sizes[i] = getApproximateSizes(range);
    }
    return sizes;
  }

  public long getApproximateSizes(final Range range) {
    final Version v = versions.getCurrent();

    final InternalKey startKey = new TransientInternalKey(ByteBuffer.wrap(range.start()),
        SequenceNumber.MAX_SEQUENCE_NUMBER, ValueType.VALUE);
    final InternalKey limitKey = new TransientInternalKey(ByteBuffer.wrap(range.limit()),
        SequenceNumber.MAX_SEQUENCE_NUMBER, ValueType.VALUE);
    final long startOffset = v.getApproximateOffsetOf(startKey);
    final long limitOffset = v.getApproximateOffsetOf(limitKey);

    return (limitOffset >= startOffset ? limitOffset - startOffset : 0);
  }

  public long getMaxNextLevelOverlappingBytes() {
    return versions.getMaxNextLevelOverlappingBytes();
  }

  private static class CompactionState {
    private final Compaction compaction;

    private final List<FileMetaData> outputs = newArrayList();

    private long smallestSnapshot;

    // State kept for output being generated
    private SequentialWriteFile outfile;
    private TableBuilder builder;

    // Current file being generated
    private long currentFileNumber;
    private long currentFileSize;
    private InternalKey currentSmallest;
    private InternalKey currentLargest;

    // private long totalBytes;

    private CompactionState(final Compaction compaction) {
      this.compaction = compaction;
    }

    public Compaction getCompaction() {
      return compaction;
    }
  }

  private static class ManualCompaction {
    private final int level;
    private final ByteBuffer begin;
    private final ByteBuffer end;

    private ManualCompaction(final int level, final ByteBuffer begin, final ByteBuffer end) {
      this.level = level;
      this.begin = begin;
      this.end = end;
    }
  }

  private static WriteBatchImpl readWriteBatch(final ByteBuffer record, final int updateSize)
      throws IOException {
    @SuppressWarnings("resource")
    final WriteBatchImpl writeBatch = new WriteBatchImpl.WriteBatchMulti();
    int entries = 0;
    while (record.hasRemaining()) {
      entries++;
      final ValueType valueType = ValueType.getValueTypeByPersistentId(record.get());
      if (valueType == VALUE) {
        final ByteBuffer key = ByteBuffers.readLengthPrefixedBytes(record);
        final ByteBuffer value = ByteBuffers.readLengthPrefixedBytes(record);
        writeBatch.put(key, value);
      } else if (valueType == DELETION) {
        final ByteBuffer key = ByteBuffers.readLengthPrefixedBytes(record);
        writeBatch.delete(key);
      } else {
        throw new IllegalStateException("Unexpected value type " + valueType);
      }
    }

    if (entries != updateSize) {
      throw new IOException(String.format("Expected %d entries in log record but found %s entries",
          updateSize, entries));
    }

    return writeBatch;
  }

  private ByteBuffer writeWriteBatch(final WriteBatchImpl updates, final long sequenceBegin) {
    // TODO send write batch straight down to logs, rework LogWriter as necessary
    final ByteBuffer record =
        options.memoryManager().allocate(SIZE_OF_LONG + SIZE_OF_INT + updates.getApproximateSize());
    record.mark();
    record.putLong(sequenceBegin);
    record.putInt(updates.size());
    updates.forEach(new Handler() {
      @Override
      public void put(final ByteBuffer key, final ByteBuffer value) {
        record.put(VALUE.getPersistentId());
        ByteBuffers.writeLengthPrefixedBytesTransparent(record, key);

        ByteBuffers.writeLengthPrefixedBytesTransparent(record, value);
      }

      @Override
      public void delete(final ByteBuffer key) {
        record.put(DELETION.getPersistentId());
        ByteBuffers.writeLengthPrefixedBytesTransparent(record, key);
      }
    });
    record.limit(record.position()).reset();
    return record;
  }

  private static class InsertIntoHandler implements Handler {
    private long sequence;
    private final MemTable memTable;

    public InsertIntoHandler(final MemTable memTable, final long sequenceBegin) {
      this.memTable = memTable;
      this.sequence = sequenceBegin;
    }

    @Override
    public void put(final ByteBuffer key, final ByteBuffer value) {
      memTable.add(new TransientInternalKey(key, sequence++, VALUE), value);
    }

    @Override
    public void delete(final ByteBuffer key) {
      memTable.add(new TransientInternalKey(key, sequence++, DELETION), ByteBuffers.EMPTY_BUFFER);
    }
  }

  @SuppressWarnings("serial")
  public static class DatabaseShutdownException extends DBException {
    public DatabaseShutdownException() {

    }

    public DatabaseShutdownException(final String message) {
      super(message);
    }
  }

  @SuppressWarnings("serial")
  public static class BackgroundProcessingException extends DBException {
    public BackgroundProcessingException(final Throwable cause) {
      super(cause);
    }
  }

  // TODO recoverable vs. nonrecoverable, tie into repair
  public static class DBException extends Exception {
    public DBException() {}

    public DBException(final Throwable cause) {
      super(cause);
    }

    public DBException(final String message) {
      super(message);
    }
  }

  private final Object suspensionMutex = new Object();
  private int suspensionCounter = 0;

  @Override
  public void suspendCompactions() throws InterruptedException {
    compactionExecutor.execute(new Runnable() {
      @Override
      public void run() {
        try {
          synchronized (suspensionMutex) {
            suspensionCounter++;
            suspensionMutex.notifyAll();
            while (suspensionCounter > 0 && !compactionExecutor.isShutdown()) {
              suspensionMutex.wait(500);
            }
          }
        } catch (final InterruptedException e) {
        }
      }
    });
    synchronized (suspensionMutex) {
      while (suspensionCounter < 1) {
        suspensionMutex.wait();
      }
    }
  }

  @Override
  public void resumeCompactions() {
    synchronized (suspensionMutex) {
      suspensionCounter--;
      suspensionMutex.notifyAll();
    }
  }

  @Override
  public void compactRange(final byte[] begin, final byte[] end) throws DBException {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public CompletionStage<DBIterator> iterator(final ReadOptions options) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CompletionStage<Long> getApproximateSize(final ByteBuffer begin, final ByteBuffer end) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CompletionStage<Void> suspendCompactions() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CompletionStage<Void> resumeCompactions() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CompletionStage<Void> compactRange(final ByteBuffer begin, final ByteBuffer end) {
    // TODO Auto-generated method stub
    return null;
  }

  private static class MemTableAndLog {
    // group the memtable and its accompanying log so that concurrent writes
    // can't be split between structures and subsequently lost in crash
    final MemTable memTable;
    final LogWriter log;
    final CompletableFuture<?> flush = new CompletableFuture<>();
    final CompletableFuture<?> swap = new CompletableFuture<>();
    DeletionHandle deletionHandle;
    volatile boolean requiresFlush = false;

    public MemTableAndLog(final MemTable memTable, final LogWriter log) {
      this.memTable = memTable;
      this.log = log;
    }

    public void markUnpersisted() {
      this.requiresFlush = true;
    }

    public boolean isUnpersisted() {
      return this.requiresFlush;
    }
  }

  private class MemTables {
    private final DeletionQueue<MemTableAndLog> immutableTables = new DeletionQueue<>();
    private volatile MemTableAndLog mutable;
    private final AtomicInteger pendingCount = new AtomicInteger(0);

    private MemTables(final MemTableAndLog first) {
      this.mutable = first;
    }

    private CompletionStage<?> submitImmutableTable(final MemTableAndLog memlog) {
      int count;
      do {
        count = pendingCount.get();
        if (count >= options.writeBufferLimit()) {
          // TODO safe recursion
          return immutableTables.peekLast().flush
              .thenCompose(voided -> submitImmutableTable(memlog));
        }
      } while (pendingCount.compareAndSet(count, count + 1));

      // we are under limit, safe to insert
      final CompletionStage<CompletionStage<?>> schedule =
          CompletableFuture.supplyAsync(() -> /* flush */null, options.env().getExecutor());
      CompletableFutures.chain(schedule, memlog.flush,
          (scheduledFlush, memlogFlush) -> CompletableFutures.chain(scheduledFlush, memlogFlush,
              (flushSuccess, memlogFlush_) -> {
                immutableTables.delete(memlog.deletionHandle);
                memlogFlush_.complete(null);
              }));
      return schedule;
    }

    public LookupResult lookup(final LookupKey key) {
      return null;
    }

    private CompletionStage<MemTableAndLog> makeRoomForWrite(final int writeUsageSize) {
      final int memtableMax = options.writeBufferSize();
      MemTableAndLog current;
      do {
        current = mutable;

        final long previousUsage = current.memTable.getAndAddApproximateMemoryUsage(writeUsageSize);
        if (previousUsage <= memtableMax && previousUsage + writeUsageSize > memtableMax) {
          // this condition can only be true for one writer
          // this writer is the one which exceeded the memtable buffer limit first
          // so here we swap the memtables

          // if the previous memtable hasn't been flushed yet, we must wait
          MemTables tables;
          while ((tables = this.memTables).immutableExists()) {
            current.acquireUpgraded(); // block other writers that might be spinning in wait for a
                                       // new
                                       // memtable
            try {
              tables.waitForImmutableCompaction();
              checkBackgroundException();
            } catch (final Throwable e) {
              current.memTable.release();
              current.release();
              throw e;
            } finally {
              current.releaseDowngraded();
            }
          }

          final long logNumber = versions.getNextFileNumber();
          try {
            this.memTables = new MemTables(
                new MemTableAndLog(
                    new MemTable(internalKeyComparator, userOptions.specifiedMemoryManager(),
                        options.memoryManager()),
                    Logs.createLogWriter(FileInfo.log(dbHandle, logNumber), logNumber, options)),
                current);
          } catch (final IOException e) {
            // we've failed to create a new memtable and update mutable/immutable
            // other writers trying to acquire it (those spinning in this loop, or new writers)
            // should
            // be fail
            setBackgroundException(e);
            current.memTable.release();
            current.release();
            throw new DBException(e);
          }

          old_scheduleCompaction();
          break;
        } else if (previousUsage < memtableMax) {
          // this write can fit into the memtable
          break;
        } else {
          // the write exceeds the memtable usage limit, but it was not the first to do so
          // loop back and acquire the new memtable.
          // incidentally, this means that the memtable's memory usage was incremented superfluously
          // it won't receive any further writes, however, and the usage changes are only relevant
          // to
          // this function

          current.memTable.release();
          current.release();
          checkBackgroundException();
        }
      } while (true);

      return current;
    }
  }

  private static class ExceptionHandler implements UncaughtExceptionHandler {
    // TODO this might be overkill, reconsider tablecache and compaction exceptions

    private final DBHandle dbHandle;
    private final Queue<Throwable> backgroundExceptions = new ConcurrentLinkedQueue<>();

    ExceptionHandler(final DBHandle dbHandle) {
      this.dbHandle = dbHandle;
    }

    @Override
    public void uncaughtException(final Thread t, final Throwable e) {
      LOGGER.error("{} error in background thread", dbHandle, e);
      backgroundExceptions.add(e);
    }

    public void checkBackgroundException() throws Throwable {
      final Throwable t = backgroundExceptions.poll();
      if (t != null) {
        throw t;
      }
    }
  }
}
