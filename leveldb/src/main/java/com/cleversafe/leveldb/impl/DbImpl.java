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

import static com.cleversafe.leveldb.impl.ValueType.DELETION;
import static com.cleversafe.leveldb.impl.ValueType.VALUE;
import static com.cleversafe.leveldb.util.CompletableFutures.closeAfter;
import static com.cleversafe.leveldb.util.SizeOf.SIZE_OF_INT;
import static com.cleversafe.leveldb.util.SizeOf.SIZE_OF_LONG;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cleversafe.leveldb.AsynchronousCloseable;
import com.cleversafe.leveldb.AsynchronousIterator;
import com.cleversafe.leveldb.DB;
import com.cleversafe.leveldb.DBIterator;
import com.cleversafe.leveldb.Env;
import com.cleversafe.leveldb.Env.DBHandle;
import com.cleversafe.leveldb.Env.LockFile;
import com.cleversafe.leveldb.FileInfo;
import com.cleversafe.leveldb.FileInfo.FileType;
import com.cleversafe.leveldb.Options;
import com.cleversafe.leveldb.ReadOptions;
import com.cleversafe.leveldb.Snapshot;
import com.cleversafe.leveldb.WriteBatch;
import com.cleversafe.leveldb.WriteOptions;
import com.cleversafe.leveldb.impl.Snapshots.SnapshotImpl;
import com.cleversafe.leveldb.impl.WriteBatchImpl.Handler;
import com.cleversafe.leveldb.table.Footer;
import com.cleversafe.leveldb.table.Table;
import com.cleversafe.leveldb.table.TableBuilder;
import com.cleversafe.leveldb.util.ByteBuffers;
import com.cleversafe.leveldb.util.Closeables;
import com.cleversafe.leveldb.util.CompletableFutures;
import com.cleversafe.leveldb.util.DeletionQueue;
import com.cleversafe.leveldb.util.DeletionQueue.DeletionHandle;
import com.cleversafe.leveldb.util.Iterators;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

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
  private final MemTables memTables;
  private final Compactions compactions = new Compactions();
  private final Snapshots snapshots = new Snapshots();
  private final AtomicBoolean shuttingDown = new AtomicBoolean();
  private final DeletionQueue<CompletableFuture<?>> openOperations = new DeletionQueue<>();

  private final Set<Long> pendingOutputs = Collections.newSetFromMap(new ConcurrentHashMap<>());

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
    compactions.maybeScheduleVersionCompaction();
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
      final int tableCacheSize = options.fileCacheSize() - 10;
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

            return env.getOwnedFiles(dbHandle)
                .thenCompose(
                    fileIter -> closeAfter(
                        Iterators.toStream(fileIter
                            .filter(fileInfo -> fileInfo.getFileType() == FileInfo.FileType.LOG
                                && ((fileInfo.getFileNumber() >= minLogNumber)
                                    || (fileInfo.getFileNumber() == previousLogNumber)))),
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

  private CompletionStage<Void> deleteObsoleteFiles() {
    return deleteObsoleteFiles(versions.getLiveFiles(), pendingOutputs.stream(),
        versions.getLogNumber(), versions.getPrevLogNumber(), versions.getManifestFileNumber(),
        tableCache, options.env(), dbHandle);
  }

  // FIXME this isn't concurrently safe
  private static CompletionStage<Void> deleteObsoleteFiles(final Stream<FileMetaData> liveFiles,
      final Stream<Long> pendingOutputs, final long logNumber, final long prevLogNumber,
      final long manifestNumber, final TableCache tableCache, final Env env,
      final DBHandle dbHandle) {
    // Make a set of all of the live files
    final Set<Long> live = Stream.concat(liveFiles.map(FileMetaData::getNumber), pendingOutputs)
        .collect(Collectors.toSet());

    @SuppressWarnings("deprecation")
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
        Iterators.toStream(fileIter.filter(shouldKeep.negate()).map(fileInfo -> {
          if (fileInfo.getFileType() == FileType.TABLE) {
            tableCache.evict(fileInfo.getFileNumber());
          }
          LOGGER.debug("{} delete type={} #{}", dbHandle, fileInfo.getFileType(),
              fileInfo.getFileNumber());
          return deleteIgnoringFileNotFound(env, fileInfo);
        })), fileIter).thenCompose(CompletableFutures::allOfVoid));
  }

  public void checkBackgroundException() {
    exceptionHandler.checkBackgroundException();
  }

  @Override
  public CompletionStage<ByteBuffer> get(final ByteBuffer key) {
    return get(key, DEFAULT_READ_OPTIONS);
  }

  @Override
  public CompletionStage<ByteBuffer> get(final ByteBuffer key, final ReadOptions readOptions) {
    checkBackgroundException();
    final DeletionHandle<CompletableFuture<?>> readOperation =
        openOperations.insert(new CompletableFuture<>());

    return getInternal(key, readOptions.verifyChecksums(), readOptions.fillCache(),
        readOptions.snapshot() != null ? ((SnapshotImpl) readOptions.snapshot()).getLastSequence()
            : versions.getLastSequence()).whenComplete((success, exception) -> {
              // don't communicate read failures to the operation future as it's only used for close
              readOperation.close();
              readOperation.item.complete(null);
            });
  }

  private CompletionStage<ByteBuffer> getInternal(final ByteBuffer key,
      final boolean verifyChecksums, final boolean fillCache, final long snapshot) {
    if (shuttingDown.get()) {
      return CompletableFutures.exceptionalFuture(
          new DatabaseShutdownException("attempted to read while shutting down database"));
    }

    final LookupKey lookupKey = new LookupKey(key, snapshot);

    final LookupResult memTableLookup = memTables.get(lookupKey);
    if (memTableLookup != null) {
      return CompletableFuture.completedFuture(memTableLookup.getValue());
    }

    // Not in memTables; try live files in level order
    // TODO checksums and cache down to tables
    return versions.get(lookupKey).thenApply(lookupResult -> {
      // possibly necessitated seek threshold compaction
      compactions.maybeScheduleVersionCompaction();
      return lookupResult.getValue();
    });
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

    if (updates.size() == 0 || updates.getApproximateSize() == 0) {
      return CompletableFuture.completedFuture(writeOptions.snapshot() ? getSnapshot() : null);
    } else {
      // TODO throttleWritesIfNecessary();

      return memTables.makeRoomForWrite(updates.getApproximateSize()).thenCompose(entry -> {
        final DeletionHandle<CompletableFuture<Snapshot>> writeOperation = entry.getKey();
        final MemTableAndLog memlog = entry.getValue();
        final long sequenceBegin = versions.getAndAddLastSequence(updates.size()) + 1;
        final CompletionStage<Void> logWrite;
        if (writeOptions.disableLog()) {
          memlog.markUnpersisted();
          logWrite = CompletableFuture.completedFuture(null);
        } else {
          logWrite =
              memlog.log.addRecord(writeWriteBatch(updates, sequenceBegin), writeOptions.sync());
        }
        logWrite.whenComplete((voidSuccess, exception) -> {
          if (exception == null) {
            updates.forEach(new InsertIntoHandler(memlog.memTable, sequenceBegin));
            writeOperation.close();
            writeOperation.item.complete(writeOptions.snapshot()
                ? snapshots.newSnapshot(sequenceBegin + updates.size() - 1) : null);
          } else {
            writeOperation.close();
            writeOperation.item.completeExceptionally(exception);
          }
        });
        return writeOperation.item;
      });
    }
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
  public SnapshotImpl getSnapshot() {
    return snapshots.newSnapshot(versions.getLastSequence());
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
        ByteBuffer.allocate(SIZE_OF_LONG + SIZE_OF_INT + updates.getApproximateSize())
            .order(ByteOrder.LITTLE_ENDIAN);
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

  @Override
  public CompletionStage<DBIterator> iterator(final ReadOptions options) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CompletionStage<Long> getApproximateSize(final ByteBuffer begin, final ByteBuffer end) {
    final Version v = versions.getCurrent();
    return v
        .getApproximateOffsetOf(
            new TransientInternalKey(begin, SequenceNumber.MAX_SEQUENCE_NUMBER, ValueType.VALUE))
        .thenCompose(
            startOffset -> v
                .getApproximateOffsetOf(new TransientInternalKey(end,
                    SequenceNumber.MAX_SEQUENCE_NUMBER, ValueType.VALUE))
            .thenApply(
                limitOffset -> (limitOffset >= startOffset ? limitOffset - startOffset : 0L)));
  }

  /**
   * ignore file not found in case of concurrent deletion
   */
  private static CompletionStage<Void> deleteIgnoringFileNotFound(final Env env,
      final FileInfo fileInfo) {
    return CompletableFutures.<Void, Void>handleExceptional(env.deleteFile(fileInfo),
        (voidSuccess, exception) -> {
          if (exception == null || exception instanceof FileNotFoundException) {
            return null;
          } else {
            throw exception;
          }
        });
  }

  private static CompletionStage<FileMetaData> buildTable(
      final Iterable<Entry<InternalKey, ByteBuffer>> data, final long fileNumber,
      final InternalKeyComparator internalKeyComparator, final DBHandle dbHandle,
      final Options options) {
    final Env env = options.env();
    final FileInfo fileInfo = FileInfo.table(dbHandle, fileNumber);
    return CompletableFutures
        .composeOnException(env.openSequentialWriteFile(fileInfo).thenCompose(file -> {

          final Iterator<Entry<InternalKey, ByteBuffer>> dataIter = data.iterator();
          Preconditions.checkArgument(dataIter.hasNext(), "cannot build from empty table");
          final Entry<InternalKey, ByteBuffer> first = dataIter.next();
          final InternalKey smallest = first.getKey();

          final TableBuilder builder = new TableBuilder(options.blockRestartInterval(),
              options.blockSize(), options.compression(), file, internalKeyComparator);
          return closeAfter(
              // TODO coalesce deletions and writes
              CompletableFutures
                  .unrollImmediate(
                      ignored -> dataIter
                          .hasNext(),
                      builder::add, first)
                  .thenCompose(largestEntry -> builder.finish()
                      .whenComplete((success, exception) -> builder.close())
                      .thenApply(fileSize -> new FileMetaData(fileNumber, fileSize,
                          smallest.heapCopy(), largestEntry.getKey().heapCopy()))),
              file);
        }), buildFailedException -> env.fileExists(fileInfo)
            .thenCompose(fileExists -> fileExists ? env.deleteFile(fileInfo)
                : CompletableFuture.completedFuture(null)))
        // verify table can be opened
        .thenCompose(fileMetaData -> Table
            .load(options.env(), dbHandle, fileNumber, internalKeyComparator,
                options.verifyChecksums(), options.compression())
            .thenCompose(Table::release).thenApply(voided -> fileMetaData));
  }

  public CompletionStage<?> flushMemTable() {
    return memTables.makeRoomForWrite(options.writeBufferSize()).thenAccept(entry -> {
      final DeletionHandle<CompletableFuture<Snapshot>> operation = entry.getKey();
      operation.close();
      operation.item.complete(null);
    });
  }

  @Override
  public CompletionStage<CompactionSuspension> suspendCompactions() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CompletionStage<Void> compactRange(final ByteBuffer begin, final ByteBuffer end) {
    // TODO Auto-generated method stub
    return null;
  }

  int numberOfFilesInLevel(final int level) {
    return versions.getCurrent().numberOfFilesInLevel(level);
  }

  public long getMaxNextLevelOverlappingBytes() {
    return versions.getMaxNextLevelOverlappingBytes();
  }

  private static class MemTableAndLog {
    // group the memtable and its accompanying log so that concurrent writes
    // can't be split between structures and subsequently lost in crash
    final MemTable memTable;
    final LogWriter log;
    DeletionHandle<?> immutableQueueHandle;
    final CompletableFuture<Void> flush = new CompletableFuture<>();
    final CompletableFuture<Void> swap = new CompletableFuture<>();
    final DeletionQueue<CompletableFuture<Snapshot>> pendingWriters = new DeletionQueue<>();
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

    public LookupResult get(final LookupKey key) {
      LookupResult result;
      if ((result = mutable.memTable.get(key)) != null) {
        return result;
      }

      for (final MemTableAndLog memlog : immutableTables) {
        if ((result = memlog.memTable.get(key)) != null) {
          return result;
        }
      }
      return null;
    }

    public CompletionStage<Entry<DeletionHandle<CompletableFuture<Snapshot>>, MemTableAndLog>> makeRoomForWrite(
        final int writeUsageSize) {
      assert writeUsageSize > 0;
      final int memtableMax = options.writeBufferSize();

      final MemTableAndLog current = mutable;
      final DeletionHandle<CompletableFuture<Snapshot>> writeOperation =
          current.pendingWriters.insert(new CompletableFuture<>());
      if (shuttingDown.get()) {
        // inserted before shutdown check to avoid race
        writeOperation.close();
        writeOperation.item.complete(null);
        return CompletableFutures.exceptionalFuture(
            new DatabaseShutdownException("attempted to write while database is shutting down"));
      }

      final long previousUsage = current.memTable.getAndAddApproximateMemoryUsage(writeUsageSize);
      if (previousUsage <= memtableMax) {
        if (previousUsage + writeUsageSize > memtableMax) {
          /*
           * this condition can only be true for one writer. this writer is the one which exceeded
           * the memtable buffer limit first, so here we swap the memtables
           */
          return swapTables(current)
              .thenApply(voided -> Maps.immutableEntry(writeOperation, current));
        } else {
          // this write can fit into the memtable
          return CompletableFuture.completedFuture(Maps.immutableEntry(writeOperation, current));
        }
      } else {
        /*
         * the write exceeds the memtable usage limit, but it was not the first to do so. wait for
         * the new memtable to be created. incidentally, this means that the memtable's memory usage
         * was incremented superfluously; it won't receive any further writes, however, and the
         * usage changes are only relevant to this function
         */
        writeOperation.close();
        writeOperation.item.complete(null);
        return current.swap.thenCompose(voided -> makeRoomForWrite(writeUsageSize));
      }
    }

    private CompletionStage<Void> swapTables(final MemTableAndLog memlog) {
      int count;
      do {
        count = pendingCount.get();
        if (count >= options.writeBufferLimit()) {
          return immutableTables.peekLast().flush.thenComposeAsync(voided -> swapTables(memlog),
              options.env().getExecutor());
        }
      } while (pendingCount.compareAndSet(count, count + 1));

      // we are under limit, safe to start a new table and to insert the immutable
      final CompletionStage<Void> createAndSwap =
          Logs.createLogWriter(FileInfo.log(dbHandle, versions.getAndIncrementNextFileNumber()),
              options.env()).thenAccept(logWriter -> {
                final MemTable newTable = new MemTable(internalKeyComparator);
                // there is a brief period of time between the next two calls during which the
                // current memlog exists in both the immutable table list and the mutable variable
                // TODO measure performance cost of fixing this with unified list
                memlog.immutableQueueHandle = immutableTables.insertFirst(memlog);
                mutable = new MemTableAndLog(newTable, logWriter);
              });

      // declare the swap successful, allowing writers waiting on a new table to proceed
      CompletableFutures.compose(createAndSwap, memlog.swap);

      // perform the memlog flush in the background while swapping the tables
      options.env().getExecutor()
          .execute(() -> CompletableFutures.compose(writeLevel0Table(memlog), memlog.flush));

      // after the flush is successful, remove the immutable table from the pending list and
      // decrement the count. we must ensure that this is done after the new table is created, as
      // that's when we insert into the immutable list and populate the handle

      // TODO check exceptions around flush
      createAndSwap.runAfterBoth(memlog.flush, () -> {
        memlog.immutableQueueHandle.close();
        pendingCount.decrementAndGet();
      });
      return createAndSwap;
    }

    private CompletionStage<Void> writeLevel0Table(final MemTableAndLog memlog) {
      // TODO check on suspended compaction
      return CompletableFuture.allOf((CompletableFuture[]) memlog.pendingWriters.toArray())
          .thenCompose(writersFinished -> {
            // skip empty mem table
            if (memlog.memTable.isEmpty()) {
              return CompletableFuture.completedFuture(null);
            }

            // write the memtable to a new sstable
            final long fileNumber = versions.getAndIncrementNextFileNumber();
            pendingOutputs.add(fileNumber);
            return buildTable(memlog.memTable.entryIterable(), fileNumber, internalKeyComparator,
                dbHandle, options)
                    .whenComplete((meta, exception) -> pendingOutputs.remove(fileNumber))
                    .thenCompose(meta -> {
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
              edit.setPreviousLogNumber(0);
              edit.setLogNumber(memlog.log.getFileNumber()); // Earlier logs no longer needed
              return versions.logAndApply(edit);
            }).thenCompose(version -> {
              compactions.maybeScheduleVersionCompaction();
              return deleteIgnoringFileNotFound(options.env(),
                  FileInfo.log(dbHandle, memlog.log.getFileNumber()));
            });
          });
    }
  }

  private class Compactions {
    // TODO in the interests of getting a viable product working, forget manual and suspended
    // compactions for now
    private final AtomicReference<CompletableFuture<?>> bgCompaction = new AtomicReference<>();

    public void maybeScheduleVersionCompaction() {
      final CompletableFuture<Void> compactionFuture;
      if (!shuttingDown.get() && versions.needsCompaction()
          && bgCompaction.compareAndSet(null, compactionFuture = new CompletableFuture<>())) {
        // only schedule if needed and one isn't already running
        options.env().getExecutor().execute(() -> {
          final DeletionHandle<CompletableFuture<?>> compactionOperation =
              openOperations.insert(compactionFuture);
          backgroundCompaction(versions.pickCompaction()).whenComplete((success, exception) -> {
            if (exception != null) {
              exceptionHandler.uncaughtException(Thread.currentThread(), exception);
            }
            bgCompaction.compareAndSet(compactionFuture, null);
            compactionOperation.close();
            compactionOperation.item.complete(null);
            maybeScheduleVersionCompaction();
          });
        });
      }
    }

    private CompletionStage<Void> backgroundCompaction(final Compaction c) {
      if (c == null) {
        return CompletableFuture.completedFuture(null);
      }
      LOGGER.debug("{} beginning compaction", this);
      if (c.isTrivialMove()) {
        final FileMetaData fileMetaData = c.input(0, 0);
        c.getEdit().deleteFile(c.getLevel(), fileMetaData.getNumber());
        c.getEdit().addFile(c.getLevel() + 1, fileMetaData);
        return versions.logAndApply(c.getEdit());
      }
      return versions.makeInputIterator(c).thenCompose(iter -> {
        final CompactionState compactionState =
            new CompactionState(c, snapshots.getOldestSequence());
        return closeAfter(closeAfter(doCompactionWork(compactionState, iter), iter).thenCompose(
            optCompactionState -> optCompactionState.map(CompactionState::installResults)
                .orElseGet(() -> CompletableFuture.completedFuture(null))),
            compactionState);
      });
    }

    /**
     * @return empty if terminated before completion
     */
    private CompletionStage<Optional<CompactionState>> doCompactionWork(
        final CompactionState compactionState,
        final AsynchronousIterator<Entry<InternalKey, ByteBuffer>> iter) {
      return CompletableFutures
          .unroll(state -> state.currentEntry.isPresent() && !shuttingDown.get(), state -> {
            final Entry<InternalKey, ByteBuffer> entry = state.currentEntry.get();

            if (state.compaction.shouldStopBefore(entry.getKey()) && state.builder != null) {
              return state.finishCompactionOutputFile()
                  .thenCompose(voided -> addIfNecessary(state, entry));
            } else {
              return addIfNecessary(state, entry);
            }
          } , iter.next().thenApply(optNext -> {
            compactionState.currentEntry = optNext;
            return compactionState;
          })).thenCompose(state -> {
            if (shuttingDown.get()) {
              return CompletableFuture.completedFuture(Optional.empty());
            } else if (state.builder != null) {
              return state.finishCompactionOutputFile().thenApply(voided -> Optional.of(state));
            } else {
              return CompletableFuture.completedFuture(Optional.of(state));
            }
          });

      // TODO port CompactionStats code
    }

    private CompletionStage<CompactionState> addIfNecessary(final CompactionState state,
        final Entry<InternalKey, ByteBuffer> entry) {
      final InternalKey key = entry.getKey();
      if (state.currentUserKey == null || internalKeyComparator.getUserComparator()
          .compare(key.getUserKey(), state.currentUserKey) != 0) {
        // First occurrence of this user key
        state.currentUserKey = key.getUserKey();
        state.lastSequenceForKey = SequenceNumber.MAX_SEQUENCE_NUMBER;
      }

      final boolean drop;
      if (state.lastSequenceForKey <= state.smallestSnapshot) {
        // Hidden by an newer entry for same user key
        drop = true; // (A)
      } else if (key.getValueType() == ValueType.DELETION
          && key.getSequenceNumber() <= state.smallestSnapshot
          && state.compaction.isBaseLevelForKey(key.getUserKey())) {

        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        // smaller sequence numbers will be dropped in the next
        // few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      } else {
        drop = false;
      }
      state.lastSequenceForKey = key.getSequenceNumber();

      if (drop) {
        return CompletableFuture.completedFuture(state);
      } else {
        // Open output file if necessary
        if (state.builder == null) {
          return state.openCompactionOutputFile(key).thenCompose(voided -> add(state, entry));
        } else {
          return add(state, entry);
        }
      }
    }

    private CompletionStage<CompactionState> add(final CompactionState state,
        final Entry<InternalKey, ByteBuffer> entry) {
      state.currentLargest = entry.getKey();
      state.builder.add(entry);

      // Close output file if it is big enough
      if (state.builder.getFileSizeEstimate() >= state.compaction.getMaxOutputFileSize()) {
        return state.finishCompactionOutputFile().thenApply(voided -> state);
      } else {
        return CompletableFuture.completedFuture(state);
      }
    }

    private class CompactionState implements AsynchronousCloseable {
      final List<FileMetaData> outputs = new ArrayList<>();
      final Compaction compaction;
      final long smallestSnapshot;

      // Current file being generated
      TableBuilder builder = null;
      long currentFileNumber = -1;
      InternalKey currentSmallest = null;
      InternalKey currentLargest = null;

      // Current operation underway
      Optional<Entry<InternalKey, ByteBuffer>> currentEntry = null;
      ByteBuffer currentUserKey = null;
      long lastSequenceForKey;

      // private long totalBytes;

      private CompactionState(final Compaction compaction, final long smallestSeqeuence) {
        this.compaction = compaction;
        this.smallestSnapshot = smallestSeqeuence;
      }

      @Override
      public CompletionStage<Void> asyncClose() {
        final CompletionStage<Void> fileClose;
        if (builder != null) {
          builder.abandon();
          builder.close();
          fileClose = builder.file.asyncClose();
        } else {
          fileClose = CompletableFuture.completedFuture(null);
        }

        pendingOutputs
            .removeAll(outputs.stream().map(FileMetaData::getNumber).collect(Collectors.toSet()));
        return fileClose;
      }

      public CompletionStage<Void> openCompactionOutputFile(final InternalKey newSmallest) {
        assert builder == null;

        final long fileNumber = versions.getAndIncrementNextFileNumber();
        pendingOutputs.add(fileNumber);
        currentFileNumber = fileNumber;
        currentSmallest = newSmallest;
        currentLargest = null;

        return options.env().openSequentialWriteFile(FileInfo.table(dbHandle, fileNumber))
            .thenAccept(writeFile -> builder = new TableBuilder(options.blockRestartInterval(),
                options.blockSize(), options.compression(), writeFile, internalKeyComparator));
      }

      public CompletionStage<Void> finishCompactionOutputFile() {
        assert builder != null;
        assert currentFileNumber > 0;

        return closeAfter(builder.finish(), builder.file).thenCompose(fileSize -> {
          outputs.add(new FileMetaData(currentFileNumber, fileSize, currentSmallest.heapCopy(),
              currentLargest.heapCopy()));
          builder = null;
          if (fileSize > Footer.ENCODED_LENGTH) {
            return Table.load(options.env(), dbHandle, currentFileNumber, internalKeyComparator,
                options.verifyChecksums(), options.compression()).thenCompose(Table::release);
          } else {
            return CompletableFuture.completedFuture(null);
          }
        });
      }

      public CompletionStage<Void> installResults() {
        // Add compaction outputs
        compaction.addInputDeletions(compaction.getEdit());
        final int level = compaction.getLevel();
        for (final FileMetaData output : outputs) {
          compaction.getEdit().addFile(level + 1, output);
        }

        return CompletableFutures
            .composeOnException(versions.logAndApply(compaction.getEdit()), exception -> {
              LOGGER.debug("{} compaction failed due to {}. will try again later", DbImpl.this,
                  exception.toString());
              return CompletableFutures.allOfVoid(outputs.stream().map(fileMetaData -> options.env()
                  .deleteFile(FileInfo.table(dbHandle, fileMetaData.getNumber()))));
            }).thenCompose(voided -> deleteObsoleteFiles());
      }
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
}
