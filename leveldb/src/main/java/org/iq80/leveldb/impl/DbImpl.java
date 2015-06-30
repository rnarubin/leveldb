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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBBufferComparator;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.Range;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.Snapshot;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.impl.Filename.FileInfo;
import org.iq80.leveldb.impl.Filename.FileType;
import org.iq80.leveldb.impl.WriteBatchImpl.Handler;
import org.iq80.leveldb.table.BytewiseComparator;
import org.iq80.leveldb.table.TableBuilder;
import org.iq80.leveldb.util.ByteBuffers;
import org.iq80.leveldb.util.Closeables;
import org.iq80.leveldb.util.InternalIterator;
import org.iq80.leveldb.util.MergingIterator;
import org.iq80.leveldb.util.MemoryManagers;
import org.iq80.leveldb.util.Snappy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.collect.Lists.newArrayList;
import static org.iq80.leveldb.impl.DbConstants.L0_SLOWDOWN_WRITES_TRIGGER;
import static org.iq80.leveldb.impl.DbConstants.L0_STOP_WRITES_TRIGGER;
import static org.iq80.leveldb.impl.DbConstants.NUM_LEVELS;
import static org.iq80.leveldb.impl.SequenceNumber.MAX_SEQUENCE_NUMBER;
import static org.iq80.leveldb.impl.ValueType.DELETION;
import static org.iq80.leveldb.impl.ValueType.VALUE;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_INT;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_LONG;

@SuppressWarnings("AccessingNonPublicFieldOfAnotherObject")
public class DbImpl
        implements DB
{
    @Override
    public String toString()
    {
        return "DbImpl [databaseDir=" + databaseDir + "]";
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(DbImpl.class);

    private final Options options;
    private final UserOptions userOptions;
    private final File databaseDir;
    private final TableCache tableCache;
    private final DbLock dbLock;
    private final VersionSet versions;

    private final AtomicBoolean shuttingDown = new AtomicBoolean();
    private final ReentrantLock mutex = new ReentrantLock();
    private final Condition backgroundCondition = mutex.newCondition();

    private final List<Long> pendingOutputs = new Vector<>();//todo

    private volatile MemTables memTables;
    private final InternalKeyComparator internalKeyComparator;

    private volatile Throwable backgroundException;
    private final UncaughtExceptionHandler backgroundExceptionHandler = new UncaughtExceptionHandler()
    {
        @Override
        public void uncaughtException(Thread t, Throwable e)
        {
            // todo need a real UncaughtExceptionHandler
            LOGGER.error("{} error in background thread {}", DbImpl.this, e);
            backgroundException = e;
        }
    };

    private final ThreadPoolExecutor compactionExecutor;
    private final BlockingQueue<Runnable> backgroundCompaction = new ArrayBlockingQueue<>(1);

    private ManualCompaction manualCompaction;

    public DbImpl(Options userOptions, File databaseDir)
            throws IOException
    {
        Preconditions.checkNotNull(userOptions, "options is null");
        Preconditions.checkNotNull(databaseDir, "databaseDir is null");
        this.userOptions = new UserOptions(userOptions);
        this.options = sanitizeOptions(userOptions);

        this.databaseDir = databaseDir;

        //use custom comparator if set
        DBBufferComparator userComparator = options.bufferComparator();
        if (userComparator == null) {
            userComparator = new BytewiseComparator();
        }
        internalKeyComparator = new InternalKeyComparator(userComparator);

        ThreadFactory compactionThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("leveldb-compaction-%s")
                .setUncaughtExceptionHandler(backgroundExceptionHandler)
                .build();
        compactionExecutor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, backgroundCompaction,
                compactionThreadFactory);

        // Reserve ten files or so for other uses and give the rest to TableCache.
        int tableCacheSize = options.maxOpenFiles() - 10;
        tableCache = new TableCache(databaseDir, tableCacheSize, internalKeyComparator, options,
                backgroundExceptionHandler);

        // create the version set

        // create the database dir if it does not already exist
        databaseDir.mkdirs();
        Preconditions.checkArgument(databaseDir.exists(), "Database directory '%s' does not exist and could not be created", databaseDir);
        Preconditions.checkArgument(databaseDir.isDirectory(), "Database directory '%s' is not a directory", databaseDir);

        mutex.lock();
        try {
            // lock the database dir
            dbLock = new DbLock(new File(databaseDir, Filename.lockFileName()));

            // verify the "current" file
            File currentFile = new File(databaseDir, Filename.currentFileName());
            if (!currentFile.canRead()) {
                Preconditions.checkArgument(options.createIfMissing(), "Database '%s' does not exist and the create if missing option is disabled", databaseDir);
            }
            else {
                Preconditions.checkArgument(!options.errorIfExists(), "Database '%s' exists and the error if exists option is enabled", databaseDir);
            }

            versions = new VersionSet(databaseDir, tableCache, internalKeyComparator, options);

            // load  (and recover) current version
            versions.recover();

            // Recover from all newer log files than the ones named in the
            // descriptor (new log files may have been added by the previous
            // incarnation without registering them in the descriptor).
            //
            // Note that PrevLogNumber() is no longer used, but we pay
            // attention to it in case we are recovering a database
            // produced by an older version of leveldb.
            long minLogNumber = versions.getLogNumber();
            long previousLogNumber = versions.getPrevLogNumber();
            List<File> filenames = Filename.listFiles(databaseDir);

            List<Long> logs = Lists.newArrayList();
            for (File filename : filenames) {
                FileInfo fileInfo = Filename.parseFileName(filename);

                if (fileInfo != null &&
                        fileInfo.getFileType() == FileType.LOG &&
                        ((fileInfo.getFileNumber() >= minLogNumber) || (fileInfo.getFileNumber() == previousLogNumber))) {
                    logs.add(fileInfo.getFileNumber());
                }
            }

            // Recover in the order in which the logs were generated
            VersionEdit edit = new VersionEdit();
            Collections.sort(logs);
            for (Long fileNumber : logs) {
                long maxSequence = recoverLogFile(fileNumber, edit);
                if (versions.getLastSequence() < maxSequence) {
                    versions.setLastSequence(maxSequence);
                }
            }

            // open transaction log
            long logFileNumber = versions.getNextFileNumber();
            LogWriter log = Logs.createLogWriter(new File(databaseDir, Filename.logFileName(logFileNumber)),
                    logFileNumber, options);
            MemTable table = new MemTable(internalKeyComparator, this.userOptions.specifiedMemoryManager(),
                    options.memoryManager());
            this.memTables = new MemTables(new MemTableAndLog(table, log), null);

            edit.setLogNumber(log.getFileNumber());

            // apply recovered edits
            versions.logAndApply(edit);

            // cleanup unused files
            deleteObsoleteFiles();

            // schedule compactions
            maybeScheduleCompaction();
            compactionExecutor.prestartAllCoreThreads();
        }
        finally {
            mutex.unlock();
        }
    }

    // deprecated wrt user-facing api
    @SuppressWarnings("deprecation")
    private static Options sanitizeOptions(Options userOptions)
    {
        Options ret = Options.copy(userOptions);
        if (ret.memoryManager() == null) {
            ret.memoryManager(MemoryManagers.heap());
        }

        if (ret.compression() != null) {
            if (ret.compression().equals(org.iq80.leveldb.CompressionType.SNAPPY)) {
                // Disable snappy if it's not available.
                ret.compression(Snappy.available() ? Snappy.instance() : null);
            }

            // should be sanitized by options insertion as well
            Preconditions.checkArgument(ret.compression().persistentId() != 0, "User compression cannot use id 0");
        }
        return ret;
    }

    private class UserOptions
    {
        private final Options options;

        public UserOptions(Options options)
        {
            this.options = options;
        }

        public boolean specifiedMemoryManager()
        {
            return this.options.memoryManager() != null;
        }
    }

    @Override
    public void close()
    {
        if (shuttingDown.getAndSet(true)) {
            return;
        }

        backgroundCompaction.clear();

        compactionExecutor.shutdown();
        try {
            compactionExecutor.awaitTermination(1, TimeUnit.DAYS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            versions.destroy();
        }
        catch (IOException e) {
            LOGGER.error("{} error in closing", this, e);
        }

        MemTables tables = memTables;
        tables.mutable.acquireAll();
        try {
            tables.mutable.log.close();
            // TODO table release
        }
        catch (IOException e) {
            LOGGER.error("{} error in closing", this, e);
        }

        if (tables.immutableExists()) {
            tables.immutable.acquireAll();
            try {
                tables.immutable.log.close();
                // TODO table release
            }
            catch (IOException e) {
                LOGGER.error("{} error in closing", this, e);
            }
        }

        tableCache.close();
        dbLock.release();
    }

    @Override
    public String getProperty(String name)
    {
        checkBackgroundException();
        return null;
    }

    private void deleteObsoleteFiles()
    {
        // Make a set of all of the live files
        List<Long> live = newArrayList(this.pendingOutputs);
        for (FileMetaData fileMetaData : versions.getLiveFiles()) {
            live.add(fileMetaData.getNumber());
        }

        for (File file : Filename.listFiles(databaseDir)) {
            FileInfo fileInfo = Filename.parseFileName(file);
            if (fileInfo == null) {
                continue;
            }
            long number = fileInfo.getFileNumber();
            boolean keep = true;
            switch (fileInfo.getFileType()) {
                case LOG:
                    keep = ((number >= versions.getLogNumber()) ||
                            (number == versions.getPrevLogNumber()));
                    break;
                case DESCRIPTOR:
                    // Keep my manifest file, and any newer incarnations'
                    // (in case there is a race that allows other incarnations)
                    keep = (number >= versions.getManifestFileNumber());
                    break;
                case TABLE:
                    keep = live.contains(number);
                    break;
                case TEMP:
                    // Any temp files that are currently being written to must
                    // be recorded in pending_outputs_, which is inserted into "live"
                    keep = live.contains(number);
                    break;
                case CURRENT:
                case DB_LOCK:
                case INFO_LOG:
                    keep = true;
                    break;
            }

            if (!keep) {
                if (fileInfo.getFileType() == FileType.TABLE) {
                    tableCache.evict(number);
                }
                LOGGER.debug("{} delete type={} #{}", this, fileInfo.getFileType(), number);
                file.delete();
            }
        }
    }

    public void flushMemTable()
    {
        makeRoomForWrite(options.writeBufferSize()).release();

        // todo bg_error code
        MemTables tables;
        while ((tables = memTables).immutableExists()) {
            tables.waitForImmutableCompaction();
        }
        checkBackgroundException();
    }

    public void compactRange(int level, ByteBuffer start, ByteBuffer end)
    {
        // TODO not thread safe
        Preconditions.checkArgument(level >= 0, "level is negative");
        Preconditions.checkArgument(level + 1 < NUM_LEVELS, "level is greater than or equal to %s", NUM_LEVELS);
        Preconditions.checkNotNull(start, "start is null");
        Preconditions.checkNotNull(end, "end is null");

        mutex.lock();
        try {
            while (this.manualCompaction != null) {
                backgroundCondition.awaitUninterruptibly();
            }
            ManualCompaction manualCompaction = new ManualCompaction(level, start, end);
            this.manualCompaction = manualCompaction;

            scheduleCompaction();

            while (this.manualCompaction == manualCompaction) {
                backgroundCondition.awaitUninterruptibly();
            }
        }
        finally {
            mutex.unlock();
        }
    }

    private void maybeScheduleCompaction()
    {
        if (!shuttingDown.get()
                && (memTables.immutableExists() || manualCompaction != null || versions.needsCompaction())) {
            scheduleCompaction();
        }
    }

    private void scheduleCompaction()
    {
        if (backgroundCompaction.offer(new Runnable()
        {
            @Override
            public void run()
            {
                mutex.lock();
                try {
                    if (!shuttingDown.get()) {
                        backgroundCompaction();
                    }
                }
                catch (DatabaseShutdownException ignored) {
                }
                catch (Throwable e) {
                    setBackgroundException(e);
                }
                finally {
                    try {
                        maybeScheduleCompaction();
                    }
                    finally {
                        backgroundCondition.signalAll();
                        mutex.unlock();
                    }
                }
            }
        })) {
            LOGGER.debug("{} scheduled compaction", this);
            /*
             * inserting into the blocking queue directly means that it is possible for a compaction to be running in the pool while another is waiting in the queue (rather than the previous
             * implementation that limited to a total of 1 compaction either running or queued). This is a compromise made to facilitate thread safety, in which compactions may be over-scheduled, in
             * favor of possibly missing a compaction of the immutable memtable
             */
        }
        else {
            LOGGER.trace("{} foregoing compaction, queue full", this);
        }
    }

    private void setBackgroundException(Throwable t)
    {
        backgroundExceptionHandler.uncaughtException(Thread.currentThread(), t);
    }

    public void checkBackgroundException()
    {
        Throwable e = backgroundException;
        if (e != null) {
            throw new BackgroundProcessingException(e);
        }
    }

    private void backgroundCompaction()
            throws IOException
    {
        LOGGER.debug("{} beginning compaction", this);

        compactMemTableInternal();

        Compaction compaction;
        if (manualCompaction != null) {
            compaction = versions.compactRange(manualCompaction.level, new TransientInternalKey(manualCompaction.begin,
                    MAX_SEQUENCE_NUMBER, ValueType.VALUE), new TransientInternalKey(manualCompaction.end, 0L,
                    ValueType.DELETION));
        }
        else {
            compaction = versions.pickCompaction();
        }

        if (compaction == null) {
            // no compaction
        }
        else if (manualCompaction == null && compaction.isTrivialMove()) {
            // Move file to next level
            Preconditions.checkState(compaction.getLevelInputs().size() == 1);
            FileMetaData fileMetaData = compaction.getLevelInputs().get(0);
            compaction.getEdit().deleteFile(compaction.getLevel(), fileMetaData.getNumber());
            compaction.getEdit().addFile(compaction.getLevel() + 1, fileMetaData);
            versions.logAndApply(compaction.getEdit());
            // log
        }
        else {
            CompactionState compactionState = new CompactionState(compaction);
            doCompactionWork(compactionState);
            cleanupCompaction(compactionState);
        }

        // manual compaction complete
        if (manualCompaction != null) {
            manualCompaction = null;
        }
    }

    private void cleanupCompaction(CompactionState compactionState)
    {
        Preconditions.checkState(mutex.isHeldByCurrentThread());

        if (compactionState.builder != null) {
            compactionState.builder.abandon();
        }
        else {
            Preconditions.checkArgument(compactionState.outfile == null);
        }

        for (FileMetaData output : compactionState.outputs) {
            pendingOutputs.remove(output.getNumber());
        }
    }

    private long recoverLogFile(long fileNumber, VersionEdit edit)
            throws IOException
    {
        Preconditions.checkState(mutex.isHeldByCurrentThread());
        File file = new File(databaseDir, Filename.logFileName(fileNumber));

        LogMonitor logMonitor = LogMonitors.logMonitor();
        try (FileInputStream fileInput = new FileInputStream(file);
                LogReader logReader = new LogReader(fileInput.getChannel(), logMonitor, true, 0,
                        options.memoryManager())) {

            LOGGER.info("{} recovering log #{}", this, fileNumber);

            // Read all the records and add to a memtable
            long maxSequence = 0;
            MemTable memTable = null;
            for (ByteBuffer record = logReader.readRecord(); record != null; record = logReader.readRecord()) {
                // read header
                if (record.remaining() < 12) {
                    logMonitor.corruption(record.remaining(), "log record too small");
                    continue;
                }
                long sequenceBegin = record.getLong();
                int updateSize = record.getInt();

                // read entries
                WriteBatchImpl writeBatch = readWriteBatch(record, updateSize);

                // apply entries to memTable
                if (memTable == null) {
                    memTable = new MemTable(internalKeyComparator, false, null);
                }
                memTable.getAndAddApproximateMemoryUsage(writeBatch.getApproximateSize());
                writeBatch.forEach(new InsertIntoHandler(memTable, sequenceBegin));
                memTable.registerCleanup(ByteBuffers.freer(record, options.memoryManager()));

                // update the maxSequence
                long lastSequence = sequenceBegin + updateSize - 1;
                if (lastSequence > maxSequence) {
                    maxSequence = lastSequence;
                }

                // flush mem table if necessary
                if (memTable.approximateMemoryUsage() > options.writeBufferSize()) {
                    try {
                        writeLevel0Table(memTable, edit, null);
                    }
                    finally {
                        memTable.release();
                    }
                    memTable = null;
                }
            }

            // flush mem table
            if (memTable != null && !memTable.isEmpty()) {
                try {
                    writeLevel0Table(memTable, edit, null);
                }
                finally {
                    memTable.release();
                }
            }

            return maxSequence;
        }
    }

    private static final ReadOptions DEFAULT_READ_OPTIONS = ReadOptions.make();
    @Override
    public byte[] get(byte[] key)
            throws DBException
    {
        return get(key, DEFAULT_READ_OPTIONS);
    }

    @Override
    public byte[] get(byte[] key, ReadOptions readOptions)
            throws DBException
    {
        checkBackgroundException();
        LookupKey lookupKey;
        mutex.lock();
        try {
            SnapshotImpl snapshot = getSnapshot(readOptions);
            lookupKey = new LookupKey(ByteBuffer.wrap(key), snapshot.getLastSequence());
            MemTables tables = memTables;

            // First look in the memtable, then in the immutable memtable (if any).
            LookupResult lookupResult = tables.mutable.memTable.get(lookupKey);
            if (lookupResult != null) {
                ByteBuffer value = lookupResult.getValue();
                if (value == null) {
                    return null;
                }
                return ByteBuffers.toArray(value);
            }
            if (tables.immutableExists()) {
                lookupResult = tables.immutable.memTable.get(lookupKey);
                if (lookupResult != null) {
                    ByteBuffer value = lookupResult.getValue();
                    if (value == null) {
                        return null;
                    }
                    return ByteBuffers.toArray(value);
                }
            }
        }
        finally {
            mutex.unlock();
        }

        // Not in memTables; try live files in level order
        LookupResult lookupResult;
        try {
            lookupResult = versions.get(lookupKey);
        }
        catch (IOException e) {
            throw new DBException(e);
        }

        // schedule compaction if necessary
        mutex.lock();
        try {
            if (versions.needsCompaction()) {
                scheduleCompaction();
            }
        }
        finally {
            mutex.unlock();
        }

        if (lookupResult != null) {
            ByteBuffer value = lookupResult.getValue();
            if (value != null) {
                return ByteBuffers.toArray(value);
            }
        }
        return null;
    }

    private static final WriteOptions DEFAULT_WRITE_OPTIONS = WriteOptions.make();
    @Override
    public void put(byte[] key, byte[] value)
            throws DBException
    {
        put(key, value, DEFAULT_WRITE_OPTIONS);
    }

    @Override
    public Snapshot put(byte[] key, byte[] value, WriteOptions writeOptions)
            throws DBException
    {
        if (userOptions.specifiedMemoryManager()) {
            return put(ByteBuffers.copy(key, options.memoryManager()),
                    ByteBuffers.copy(value, options.memoryManager()), writeOptions);
        }
        else {
            return put(ByteBuffer.wrap(key), ByteBuffer.wrap(value), writeOptions);
        }
    }

    public Snapshot put(ByteBuffer key, ByteBuffer value)
            throws DBException
    {
        return put(key, value, DEFAULT_WRITE_OPTIONS);
    }

    public Snapshot put(ByteBuffer key, ByteBuffer value, WriteOptions writeOptions)
            throws DBException
    {
        return writeInternal(new WriteBatchImpl.WriteBatchSingle(key, value), writeOptions);
    }

    @Override
    public void delete(byte[] key)
            throws DBException
    {
        delete(key, DEFAULT_WRITE_OPTIONS);
    }

    @Override
    public Snapshot delete(byte[] key, WriteOptions writeOptions)
            throws DBException
    {
        if (userOptions.specifiedMemoryManager()) {
            return delete(ByteBuffers.copy(key, options.memoryManager()), writeOptions);
        }
        else {
            return delete(ByteBuffer.wrap(key), writeOptions);
        }
    }

    public Snapshot delete(ByteBuffer key)
            throws DBException
    {
        return delete(key, DEFAULT_WRITE_OPTIONS);
    }

    public Snapshot delete(ByteBuffer key, WriteOptions options)
            throws DBException
    {
        return writeInternal(new WriteBatchImpl.WriteBatchSingle(key), options);
    }

    @Override
    public void write(WriteBatch updates)
            throws DBException
    {
        writeInternal((WriteBatchImpl) updates, DEFAULT_WRITE_OPTIONS);
    }

    @Override
    public Snapshot write(WriteBatch updates, WriteOptions options)
            throws DBException
    {
        return writeInternal((WriteBatchImpl) updates, options);
    }

    public Snapshot writeInternal(WriteBatchImpl updates, WriteOptions writeOptions)
            throws DBException
    {
        checkBackgroundException();
        long sequenceEnd;
        if (updates.size() != 0) {

            throttleWritesIfNecessary();

            MemTableAndLog memlog = makeRoomForWrite(updates.getApproximateSize());

            try {
                // Get sequence numbers for this change set
                final long sequenceDelta = updates.size();
                final long sequenceBegin = versions.getAndAddLastSequence(sequenceDelta) + 1;
                sequenceEnd = sequenceBegin + sequenceDelta - 1;

                // Log write
                ByteBuffer record = writeWriteBatch(updates, sequenceBegin);
                try {
                    memlog.log.addRecord(record, writeOptions.sync());
                }
                catch (IOException e) {
                    throw Throwables.propagate(e);
                }
                finally {
                    options.memoryManager().free(record);
                }

                // Update memtable
                updates.forEach(new InsertIntoHandler(memlog.memTable, sequenceBegin));
            }
            finally {
                memlog.release();
            }
        }
        else {
            sequenceEnd = versions.getLastSequence();
        }

        if (writeOptions.snapshot()) {
            return new SnapshotImpl(versions.getCurrent(), sequenceEnd);
        }
        else {
            return null;
        }
    }

    @Override
    public WriteBatch createWriteBatch()
    {
        checkBackgroundException();
        return new WriteBatchImpl.WriteBatchMulti();
    }

    @Override
    public SeekingIteratorAdapter iterator()
    {
        return iterator(DEFAULT_READ_OPTIONS);
    }

    public SeekingIteratorAdapter iterator(ReadOptions readOptions)
    {
        checkBackgroundException();
        mutex.lock();
        try {
            MergingIterator rawIterator = internalIterator();

            // filter any entries not visible in our snapshot
            SnapshotImpl snapshot = getSnapshot(readOptions);
            SnapshotSeekingIterator snapshotIterator = new SnapshotSeekingIterator(rawIterator, snapshot,
                    internalKeyComparator.getUserComparator());
            return new SeekingIteratorAdapter(snapshotIterator);
        }
        catch (IOException e) {
            throw new DBException(e);
        }
        finally {
            mutex.unlock();
        }
    }

    MergingIterator internalIterator()
            throws IOException
    {
        mutex.lock();
        try {
            // merge together the memTable, immutableMemTable, and tables in version set
            List<InternalIterator> iterators = new ArrayList<>();
            MemTable immutable = null, mutable;
            do {
                MemTables tables = memTables;
                if (tables.immutableExists() && (immutable = tables.immutable.memTable.retain()) == null) {
                    continue;
                }
                if ((mutable = tables.mutable.memTable.retain()) == null) {
                    Closeables.closeIO(immutable);
                    continue;
                }
                break;
            }
            while (true);

            if (immutable != null) {
                iterators.add(immutable.iterator());
            }
            iterators.add(mutable.iterator());
            Closeables.closeIO(mutable, immutable);

            Version current = versions.getCurrent();
            iterators.addAll(current.getLevel0Files());
            iterators.addAll(current.getLevelIterators());
            return new MergingIterator(iterators, internalKeyComparator);
        }
        finally {
            mutex.unlock();
        }
    }

    public Snapshot getSnapshot()
    {
        checkBackgroundException();
        mutex.lock();
        try {
            return new SnapshotImpl(versions.getCurrent(), versions.getLastSequence());
        }
        finally {
            mutex.unlock();
        }
    }

    private SnapshotImpl getSnapshot(ReadOptions options)
    {
        SnapshotImpl snapshot;
        if (options.snapshot() != null) {
            snapshot = (SnapshotImpl) options.snapshot();
        }
        else {
            snapshot = new SnapshotImpl(versions.getCurrent(), versions.getLastSequence());
            snapshot.close(); // To avoid holding the snapshot active..
        }
        return snapshot;
    }

    private void throttleWritesIfNecessary()
    {
        if (!this.options.throttleLevel0()) {
            return;
        }
        boolean allowDelay = true;
        while (true) {
            if (allowDelay && versions.numberOfFilesInLevel(0) > L0_SLOWDOWN_WRITES_TRIGGER) {
                // We are getting close to hitting a hard limit on the number of
                // L0 files.  Rather than delaying a single write by several
                // seconds when we hit the hard limit, start delaying each
                // individual write by 1ms to reduce latency variance.  Also,
                // this delay hands over some CPU to the compaction thread in
                // case it is sharing the same core as the writer.
                try {
                    Thread.sleep(1);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                allowDelay = false;
            }
            if (versions.numberOfFilesInLevel(0) >= L0_STOP_WRITES_TRIGGER) {
                try {
                    mutex.lock();
                    backgroundCondition.awaitUninterruptibly();
                }
                finally {
                    mutex.unlock();
                }
            }
            else {
                break;
            }
        }
    }

    private MemTableAndLog makeRoomForWrite(int writeUsageSize)
    {
        int memtableMax = options.writeBufferSize();
        MemTableAndLog current;
        do {
            current = this.memTables.mutable.acquire();
            long previousUsage = current.memTable.getAndAddApproximateMemoryUsage(writeUsageSize);
            if (previousUsage <= memtableMax && previousUsage + writeUsageSize > memtableMax) {
                //this condition can only be true for one writer
                //this writer is the one which exceeded the memtable buffer limit first
                //so here we swap the memtables

                //if the previous memtable hasn't been flushed yet, we must wait
                MemTables tables;
                while ((tables = this.memTables).immutableExists()) {
                    current.acquireUpgraded(); // block other writers that might be spinning in wait for a new memtable
                    try {
                        tables.waitForImmutableCompaction();
                        checkBackgroundException();
                    }
                    catch (Throwable e) {
                        current.release();
                        throw e;
                    }
                    finally {
                        current.releaseDowngraded();
                    }
                }

                long logNumber = versions.getNextFileNumber();
                try {
                    this.memTables = new MemTables(new MemTableAndLog(new MemTable(internalKeyComparator,
                            userOptions.specifiedMemoryManager(), options.memoryManager()), Logs.createLogWriter(
                            new File(databaseDir, Filename.logFileName(logNumber)), logNumber, options)),
                            current);
                }
                catch (IOException e) {
                    //we've failed to create a new memtable and update mutable/immutable
                    //other writers trying to acquire it (those spinning in this loop, or new writers) should be fail
                    setBackgroundException(e);
                    current.release();
                    throw new DBException(e);
                }

                scheduleCompaction();
                break;
            }
            else if (previousUsage < memtableMax) {
                //this write can fit into the memtable
                break;
            }
            else {
                //the write exceeds the memtable usage limit, but it was not the first to do so
                //loop back and acquire the new memtable.
                //incidentally, this means that the memtable's memory usage was incremented superfluously
                //it won't receive any further writes, however, and the usage changes are only relevant to this function

                current.release();
                checkBackgroundException();
            }
        }
        while (true);

        return current;
    }

    public void compactMemTable()
            throws IOException
    {
        compactMemTableInternal();
    }

    private void compactMemTableInternal()
            throws IOException
    {
        MemTables tables = this.memTables;
        if (!tables.claimedForFlush.compareAndSet(false, true)) {
            return;
        }

        // Save the contents of the memtable as a new Table

        //block until pending writes have finished
        MemTableAndLog immutable = tables.immutable.acquireAll();
        try {
            immutable.log.close();
            VersionEdit edit = new VersionEdit();
            Version base = versions.getCurrent();
            LOGGER.debug("{} flushing {}", this, immutable.memTable);
            writeLevel0Table(immutable.memTable, edit, base);

            if (shuttingDown.get()) {
                throw new DatabaseShutdownException("Database shutdown during memtable compaction");
            }

            edit.setPreviousLogNumber(0);
            edit.setLogNumber(immutable.log.getFileNumber());  // Earlier logs no longer needed
            versions.logAndApply(edit);

            this.memTables = new MemTables(this.memTables.mutable, null);
            immutable.memTable.release();
        }
        finally {
            try {
                tables.finishCompaction();
            }
            finally {
                immutable.releaseAll();
            }
        }

        deleteObsoleteFiles();
    }

    private void writeLevel0Table(MemTable mem, VersionEdit edit, Version base)
            throws IOException
    {

        // skip empty mem table
        if (mem.isEmpty()) {
            return;
        }

        // write the memtable to a new sstable
        long fileNumber = versions.getNextFileNumber();
        pendingOutputs.add(fileNumber);
        FileMetaData meta;
        meta = buildTable(mem.simpleIterable(), fileNumber);
        pendingOutputs.remove(fileNumber);

        // Note that if file size is zero, the file has been deleted and
        // should not be added to the manifest.
        int level = 0;
        if (meta != null && meta.getFileSize() > 0) {
            ByteBuffer minUserKey = meta.getSmallest().getUserKey();
            ByteBuffer maxUserKey = meta.getLargest().getUserKey();
            if (base != null) {
                level = base.pickLevelForMemTableOutput(minUserKey, maxUserKey);
            }
            edit.addFile(level, meta);
        }
    }

    private FileMetaData buildTable(Iterable<Entry<InternalKey, ByteBuffer>> data, long fileNumber)
            throws IOException
    {
        File file = new File(databaseDir, Filename.tableFileName(fileNumber));
        try {
            InternalKey smallest = null;
            InternalKey largest = null;

            try (FileOutputStream fileOutput = new FileOutputStream(file);
                    FileChannel channel = fileOutput.getChannel()) {
                try (TableBuilder tableBuilder = new TableBuilder(options, channel, internalKeyComparator)) {
                    for (Entry<InternalKey, ByteBuffer> entry : data) {
                        // update keys
                        InternalKey key = entry.getKey();
                        if (smallest == null) {
                            smallest = key;
                        }
                        largest = key;

                        tableBuilder.add(key, entry.getValue());
                    }

                    tableBuilder.finish();
                }
                finally {
                   channel.force(true);
                }
            }

            if (smallest == null) {
                return null;
            }
            FileMetaData fileMetaData = new FileMetaData(fileNumber, file.length(), smallest.heapCopy(),
                    largest.heapCopy());

            // verify table can be opened
            tableCache.newIterator(fileMetaData).close();

            pendingOutputs.remove(fileNumber);

            return fileMetaData;
        }
        catch (IOException e) {
            file.delete();
            throw e;
        }
    }

    private void doCompactionWork(CompactionState compactionState)
            throws IOException
    {
        Preconditions.checkState(mutex.isHeldByCurrentThread());
        Preconditions.checkArgument(versions.numberOfBytesInLevel(compactionState.getCompaction().getLevel()) > 0);
        Preconditions.checkArgument(compactionState.builder == null);
        Preconditions.checkArgument(compactionState.outfile == null);

        // todo track snapshots
        compactionState.smallestSnapshot = versions.getLastSequence();

        // Release mutex while we're actually doing the compaction work
        mutex.unlock();
        try {
            try(MergingIterator iterator = versions.makeInputIterator(compactionState.compaction)){

                ByteBuffer currentUserKey = null;
                boolean hasCurrentUserKey = false;

                long lastSequenceForKey = MAX_SEQUENCE_NUMBER;
                while (iterator.hasNext() && !shuttingDown.get()) {
                    // always give priority to compacting the current mem table
                    compactMemTableInternal();

                    InternalKey key = iterator.peek().getKey();
                    if (compactionState.compaction.shouldStopBefore(key) && compactionState.builder != null) {
                        finishCompactionOutputFile(compactionState);
                    }

                    // Handle key/value, add to state, etc.
                    boolean drop = false;
                    // todo if key doesn't parse (it is corrupted),
                    /*
                    if (false
                    //!ParseInternalKey(key, &ikey)) {
                        // do not hide error keys
                        currentUserKey = null;
                        hasCurrentUserKey = false;
                        lastSequenceForKey = MAX_SEQUENCE_NUMBER;
                    }
                    else
                    */
                    {
                        if (!hasCurrentUserKey || internalKeyComparator.getUserComparator().compare(key.getUserKey(), currentUserKey) != 0) {
                            // First occurrence of this user key
                            currentUserKey = ByteBuffers.heapCopy(key.getUserKey());
                            hasCurrentUserKey = true;
                            lastSequenceForKey = MAX_SEQUENCE_NUMBER;
                        }

                        if (lastSequenceForKey <= compactionState.smallestSnapshot) {
                            // Hidden by an newer entry for same user key
                            drop = true; // (A)
                        }
                        else if (key.getValueType() == ValueType.DELETION &&
                                key.getSequenceNumber() <= compactionState.smallestSnapshot &&
                                compactionState.compaction.isBaseLevelForKey(key.getUserKey())) {

                            // For this user key:
                            // (1) there is no data in higher levels
                            // (2) data in lower levels will have larger sequence numbers
                            // (3) data in layers that are being compacted here and have
                            //     smaller sequence numbers will be dropped in the next
                            //     few iterations of this loop (by rule (A) above).
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
                        if (compactionState.builder.getFileSize() >=
                                compactionState.compaction.getMaxOutputFileSize()) {
                            compactionState.currentLargest = compactionState.currentLargest.heapCopy();
                            finishCompactionOutputFile(compactionState);
                        }
                    }
                    iterator.next();
                }
                compactionState.currentLargest = compactionState.currentLargest.heapCopy();
            }

            if (shuttingDown.get()) {
                throw new DatabaseShutdownException("DB shutdown during compaction");
            }
            if (compactionState.builder != null) {
                finishCompactionOutputFile(compactionState);
            }
        }
        finally {
            mutex.lock();
        }

        // todo port CompactionStats code

        installCompactionResults(compactionState);
    }

    @SuppressWarnings("resource")
    private void openCompactionOutputFile(CompactionState compactionState)
            throws FileNotFoundException
    {
        Preconditions.checkNotNull(compactionState, "compactionState is null");
        Preconditions.checkArgument(compactionState.builder == null, "compactionState builder is not null");

        mutex.lock();
        try {
            long fileNumber = versions.getNextFileNumber();
            pendingOutputs.add(fileNumber);
            compactionState.currentFileNumber = fileNumber;
            compactionState.currentFileSize = 0;
            compactionState.currentSmallest = null;
            compactionState.currentLargest = null;

            File file = new File(databaseDir, Filename.tableFileName(fileNumber));
            compactionState.outfile = new FileOutputStream(file).getChannel();
            compactionState.builder = new TableBuilder(options, compactionState.outfile, internalKeyComparator);
        }
        finally {
            mutex.unlock();
        }
    }

    private void finishCompactionOutputFile(CompactionState compactionState)
            throws IOException
    {
        Preconditions.checkNotNull(compactionState, "compactionState is null");
        Preconditions.checkArgument(compactionState.outfile != null);
        Preconditions.checkArgument(compactionState.builder != null);

        long outputNumber = compactionState.currentFileNumber;
        Preconditions.checkArgument(outputNumber != 0);

        long currentEntries = compactionState.builder.getEntryCount();
        compactionState.builder.finish();

        long currentBytes = compactionState.builder.getFileSize();
        compactionState.currentFileSize = currentBytes;
        // compactionState.totalBytes += currentBytes;

        FileMetaData currentFileMetaData = new FileMetaData(compactionState.currentFileNumber,
                compactionState.currentFileSize,
                compactionState.currentSmallest,
                compactionState.currentLargest);
        compactionState.outputs.add(currentFileMetaData);

        compactionState.builder.close();
        compactionState.builder = null;

        compactionState.outfile.force(true);
        compactionState.outfile.close();
        compactionState.outfile = null;

        if (currentEntries > 0) {
            // Verify that the table is usable
            tableCache.newIterator(outputNumber).close();
        }
    }

    private void installCompactionResults(CompactionState compact)
    {
        Preconditions.checkState(mutex.isHeldByCurrentThread());

        // Add compaction outputs
        compact.compaction.addInputDeletions(compact.compaction.getEdit());
        int level = compact.compaction.getLevel();
        for (FileMetaData output : compact.outputs) {
            compact.compaction.getEdit().addFile(level + 1, output);
            pendingOutputs.remove(output.getNumber());
        }

        try {
            versions.logAndApply(compact.compaction.getEdit());
            deleteObsoleteFiles();
        }
        catch (IOException e) {
            LOGGER.debug("{} compaction failed due to {}. will try again later", this, e.toString());
            // Compaction failed for some reason.  Simply discard the work and try again later.

            // Discard any files we may have created during this failed compaction
            for (FileMetaData output : compact.outputs) {
                File file = new File(databaseDir, Filename.tableFileName(output.getNumber()));
                file.delete();
            }
            compact.outputs.clear();
        }
    }

    int numberOfFilesInLevel(int level)
    {
        return versions.getCurrent().numberOfFilesInLevel(level);
    }

    @Override
    public long[] getApproximateSizes(Range... ranges)
    {
        Preconditions.checkNotNull(ranges, "ranges is null");
        long[] sizes = new long[ranges.length];
        for (int i = 0; i < ranges.length; i++) {
            Range range = ranges[i];
            sizes[i] = getApproximateSizes(range);
        }
        return sizes;
    }

    public long getApproximateSizes(Range range)
    {
        Version v = versions.getCurrent();

        InternalKey startKey = new TransientInternalKey(ByteBuffer.wrap(range.start()),
                SequenceNumber.MAX_SEQUENCE_NUMBER, ValueType.VALUE);
        InternalKey limitKey = new TransientInternalKey(ByteBuffer.wrap(range.limit()),
                SequenceNumber.MAX_SEQUENCE_NUMBER, ValueType.VALUE);
        long startOffset = v.getApproximateOffsetOf(startKey);
        long limitOffset = v.getApproximateOffsetOf(limitKey);

        return (limitOffset >= startOffset ? limitOffset - startOffset : 0);
    }

    public long getMaxNextLevelOverlappingBytes()
    {
        return versions.getMaxNextLevelOverlappingBytes();
    }

    private static class CompactionState
    {
        private final Compaction compaction;

        private final List<FileMetaData> outputs = newArrayList();

        private long smallestSnapshot;

        // State kept for output being generated
        private FileChannel outfile;
        private TableBuilder builder;

        // Current file being generated
        private long currentFileNumber;
        private long currentFileSize;
        private InternalKey currentSmallest;
        private InternalKey currentLargest;

        // private long totalBytes;

        private CompactionState(Compaction compaction)
        {
            this.compaction = compaction;
        }

        public Compaction getCompaction()
        {
            return compaction;
        }
    }

    private static class ManualCompaction
    {
        private final int level;
        private final ByteBuffer begin;
        private final ByteBuffer end;

        private ManualCompaction(int level, ByteBuffer begin, ByteBuffer end)
        {
            this.level = level;
            this.begin = begin;
            this.end = end;
        }
    }

    private WriteBatchImpl readWriteBatch(ByteBuffer record, int updateSize)
            throws IOException
    {
        @SuppressWarnings("resource")
        WriteBatchImpl writeBatch = new WriteBatchImpl.WriteBatchMulti();
        int entries = 0;
        while (record.hasRemaining()) {
            entries++;
            ValueType valueType = ValueType.getValueTypeByPersistentId(record.get());
            if (valueType == VALUE) {
                ByteBuffer key = ByteBuffers.readLengthPrefixedBytes(record);
                ByteBuffer value = ByteBuffers.readLengthPrefixedBytes(record);
                // TODO trace write batch internal keys
                writeBatch.put(key, value);
            }
            else if (valueType == DELETION) {
                ByteBuffer key = ByteBuffers.readLengthPrefixedBytes(record);
                writeBatch.delete(key);
            }
            else {
                throw new IllegalStateException("Unexpected value type " + valueType);
            }
        }

        if (entries != updateSize) {
            throw new IOException(String.format("Expected %d entries in log record but found %s entries", updateSize, entries));
        }

        return writeBatch;
    }

    private ByteBuffer writeWriteBatch(WriteBatchImpl updates, long sequenceBegin)
    {
        // TODO send write batch straight down to logs, rework LogWriter as necessary
        final ByteBuffer record = options.memoryManager().allocate(
                SIZE_OF_LONG + SIZE_OF_INT + updates.getApproximateSize());
        record.mark();
        record.putLong(sequenceBegin);
        record.putInt(updates.size());
        updates.forEach(new Handler()
        {
            @Override
            public void put(ByteBuffer key, ByteBuffer value)
            {
                record.put(VALUE.getPersistentId());
                ByteBuffers.writeLengthPrefixedBytesTransparent(record, key);

                ByteBuffers.writeLengthPrefixedBytesTransparent(record, value);
            }

            @Override
            public void delete(ByteBuffer key)
            {
                record.put(DELETION.getPersistentId());
                ByteBuffers.writeLengthPrefixedBytesTransparent(record, key);
            }
        });
        record.limit(record.position()).reset();
        return record;
    }

    private class InsertIntoHandler
            implements Handler
    {
        private long sequence;
        private final MemTable memTable;

        public InsertIntoHandler(MemTable memTable, long sequenceBegin)
        {
            this.memTable = memTable;
            this.sequence = sequenceBegin;
        }

        @Override
        public void put(ByteBuffer key, ByteBuffer value)
        {
            // FIXME transient key freeing
            memTable.add(new TransientInternalKey(key, sequence++, VALUE), value);
        }

        @Override
        public void delete(ByteBuffer key)
        {
            // FIXME transient key freeing
            memTable.add(new TransientInternalKey(key, sequence++, DELETION), ByteBuffers.EMPTY_BUFFER);
        }
    }

    @SuppressWarnings("serial")
    public static class DatabaseShutdownException
            extends DBException
    {
        public DatabaseShutdownException()
        {
        }

        public DatabaseShutdownException(String message)
        {
            super(message);
        }
    }

    @SuppressWarnings("serial")
    public static class BackgroundProcessingException
            extends DBException
    {
        public BackgroundProcessingException(Throwable cause)
        {
            super(cause);
        }
    }

    private Object suspensionMutex = new Object();
    private int suspensionCounter = 0;

    @Override
    public void suspendCompactions()
            throws InterruptedException
    {
        compactionExecutor.execute(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    synchronized (suspensionMutex) {
                        suspensionCounter++;
                        suspensionMutex.notifyAll();
                        while (suspensionCounter > 0 && !compactionExecutor.isShutdown()) {
                            suspensionMutex.wait(500);
                        }
                    }
                }
                catch (InterruptedException e) {
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
    public void resumeCompactions()
    {
        synchronized (suspensionMutex) {
            suspensionCounter--;
            suspensionMutex.notifyAll();
        }
    }

    @Override
    public void compactRange(byte[] begin, byte[] end)
            throws DBException
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    private static class MemTableAndLog
    {
        //group the memtable and its accompanying log so that concurrent writes
        //can't be split between structures and subsequently lost in crash
        public final MemTable memTable;
        public final LogWriter log;
        private static final int maxPermits = Integer.MAX_VALUE;

        // serves as read/write lock with slightly more flexibility
        private final Semaphore semaphore;

        public MemTableAndLog(MemTable memTable, LogWriter log)
        {
            this.memTable = memTable;
            this.log = log;
            this.semaphore = new Semaphore(maxPermits);
        }

        public MemTableAndLog acquire()
        {
            this.semaphore.acquireUninterruptibly();
            return this;
        }

        public void release()
        {
            this.semaphore.release();
        }

        public MemTableAndLog acquireAll()
        {
            this.semaphore.acquireUninterruptibly(maxPermits);
            return this;
        }

        public void releaseAll()
        {
            this.semaphore.release(maxPermits);
        }

        /**
         * upgrade from read lock to write lock
         */
        public MemTableAndLog acquireUpgraded()
        {
            this.semaphore.acquireUninterruptibly(maxPermits - 1);
            return this;
        }

        public void releaseDowngraded()
        {
            this.semaphore.release(maxPermits - 1);
        }
    }

    private static class MemTables
    {
        final MemTableAndLog mutable, immutable;
        private final CountDownLatch immutableHasFlushed;
        final AtomicBoolean claimedForFlush;

        public MemTables(final MemTableAndLog mutable, MemTableAndLog immutable)
        {
            this.mutable = mutable;
            this.immutable = immutable;
            this.immutableHasFlushed = new CountDownLatch(1);
            this.claimedForFlush = new AtomicBoolean(false);
            if (immutable == null) {
                claimedForFlush.set(true);
                finishCompaction();
            }
        }

        void finishCompaction()
        {
            this.immutableHasFlushed.countDown();
        }

        boolean immutableExists()
        {
            return this.immutableHasFlushed.getCount() > 0;
        }

        void waitForImmutableCompaction()
        {
            try {
                this.immutableHasFlushed.await();
            }
            catch (InterruptedException e) {
                throw new DBException(e);
            }
        }

        @Override
        public String toString()
        {
            return "MemTables [mutable=" + mutable + ", immutable=" + immutable + "]";
        }
    }
}
