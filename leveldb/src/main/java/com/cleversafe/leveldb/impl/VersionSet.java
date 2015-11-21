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

import static com.cleversafe.leveldb.impl.DbConstants.L0_COMPACTION_TRIGGER;
import static com.cleversafe.leveldb.impl.DbConstants.NUM_LEVELS;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cleversafe.leveldb.AsynchronousCloseable;
import com.cleversafe.leveldb.Env;
import com.cleversafe.leveldb.Env.DBHandle;
import com.cleversafe.leveldb.Env.SequentialReadFile;
import com.cleversafe.leveldb.FileInfo;
import com.cleversafe.leveldb.util.Closeables;
import com.cleversafe.leveldb.util.CompletableFutures;
import com.cleversafe.leveldb.util.GrowingBuffer;
import com.cleversafe.leveldb.util.MemoryManagers;
import com.cleversafe.leveldb.util.MergingIterator;
import com.cleversafe.leveldb.util.SeekingAsynchronousIterator;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;

public class VersionSet implements AsynchronousCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(VersionSet.class);

  private final AtomicLong nextFileNumber;
  private final AtomicLong lastSequence;
  private final long manifestFileNumber;
  private long logNumber;
  private long prevLogNumber;
  private final AtomicReference<Version> current = new AtomicReference<>(null);
  private final AtomicReference<CompletionStage<Void>> logAndApplyMutex =
      new AtomicReference<>(CompletableFuture.completedFuture(null));

  private final Set<Version> activeVersions =
      Collections.newSetFromMap(new MapMaker().weakKeys().makeMap());
  private final DBHandle dbHandle;
  private final TableCache tableCache;
  private final InternalKeyComparator internalKeyComparator;
  private final Env env;

  private LogWriter descriptorLog;

  private final InternalKey[] compactPointers;

  private VersionSet(final DBHandle dbHandle, final TableCache tableCache,
      final InternalKeyComparator internalKeyComparator, final Env env, final long nextFileNumber,
      final long manifestFileNumber, final long lastSequence, final long logNumber,
      final long prevLogNumber, final Version initialVersion, final InternalKey[] compactPointers) {
    this.dbHandle = dbHandle;
    this.tableCache = tableCache;
    this.internalKeyComparator = internalKeyComparator;
    this.env = env;
    this.nextFileNumber = new AtomicLong(nextFileNumber);
    this.lastSequence = new AtomicLong(lastSequence);
    this.manifestFileNumber = manifestFileNumber;
    this.logNumber = logNumber;
    this.prevLogNumber = prevLogNumber;
    this.compactPointers = compactPointers;
    appendVersion(null, initialVersion);
  }

  /**
   * if the CURRENT file exists, recovers the version set from the existing manifest. otherwise,
   * creates a new manifest and version set
   */
  public static CompletionStage<VersionSet> newVersionSet(final DBHandle dbHandle,
      final TableCache tableCache, final InternalKeyComparator internalKeyComparator,
      final Env env) {
    return env.fileExists(FileInfo.current(dbHandle)).thenCompose(exists -> {
      if (exists) {
        return recover(dbHandle, env, tableCache, internalKeyComparator);
      } else {
        // create new initial manifest file
        final VersionEdit edit = new VersionEdit();
        edit.setComparatorName(internalKeyComparator.getUserComparator().name());
        edit.setLastSequenceNumber(0L);
        edit.setLogNumber(0L);
        final long manifestNum = 1L;
        edit.setNextFileNumber(2L);
        final GrowingBuffer record = edit.encode(MemoryManagers.heap());

        return Logs.createLogWriter(FileInfo.manifest(dbHandle, manifestNum), env)
            .thenCompose(logWriter -> CompletableFutures.composeUnconditionally(
                logWriter.addRecord(record.get(), true),
                voided -> logWriter.asyncClose()
                    .whenComplete((close, closeException) -> record.close())))
            .thenCompose(voided -> setCurrentFile(env, dbHandle, manifestNum))
            .thenApply(voided -> new VersionSet(dbHandle, tableCache, internalKeyComparator, env,
                2L, 1L, 0L, 0L, 0L,
                new Version(new FileMetaData[][] {{}}, 0, 0, tableCache, internalKeyComparator),
                new InternalKey[NUM_LEVELS]));
      }
    });
  }

  private static CompletionStage<VersionSet> recover(final DBHandle dbHandle, final Env env,
      final TableCache tableCache, final InternalKeyComparator internalKeyComparator) {
    // Read "CURRENT" file, which contains a pointer to the current manifest file
    return readStringFromFile(env, FileInfo.current(dbHandle)).thenCompose(currentName -> {
      if (currentName.isEmpty()) {
        throw new IllegalStateException("CURRENT name is empty");
      }
      final int newLineIndex = currentName.indexOf('\n');
      if (newLineIndex != currentName.length() - 1) {
        throw new IllegalStateException("CURRENT file's first newline is not at end of file");
      }
      currentName = currentName.substring(0, newLineIndex);

      return LogReader
          .newLogReader(env, FileInfo.manifest(dbHandle, descriptorFileNumber(currentName)),
              LogMonitors.throwExceptionMonitor(), true, 0)
          .thenCompose(manifestReader -> {
        class ValStruct {
          long nextFileNumber = 1L;
          long lastSequence = 0L;
          long logNumber = 0L;
          long prevLogNumber = 0L;
          final VersionBuilder versionBuilder =
              new VersionBuilder(internalKeyComparator, tableCache, new FileMetaData[][] {{}});
          final InternalKey[] compactedPointers = new InternalKey[NUM_LEVELS];
        }
        return CompletableFutures.<ValStruct>closeAfter(
            manifestReader.<ValStruct>reduce(new ValStruct(), (vals, record) -> {
          final VersionEdit edit = new VersionEdit(record);

          final String editComparator = edit.getComparatorName();
          final String userComparator = internalKeyComparator.getUserComparator().name();
          Preconditions.checkArgument(
              editComparator == null || editComparator.equals(userComparator),
              "Expected user comparator %s to match existing database comparator %s",
              userComparator, editComparator);

          VersionEdit.mergeCompactPointers(edit.getCompactPointers(), vals.compactedPointers);

          vals.versionBuilder.apply(edit);

          vals.nextFileNumber = edit.getNextFileNumber().orElse(vals.nextFileNumber);
          vals.lastSequence = edit.getLastSequenceNumber().orElse(vals.lastSequence);
          vals.logNumber = edit.getLogNumber().orElse(vals.logNumber);
          vals.prevLogNumber = edit.getPreviousLogNumber().orElse(vals.prevLogNumber);

          return vals;
        }), manifestReader)
            // TODO call corruption if missing vals
            .thenApply(vals -> new VersionSet(dbHandle, tableCache, internalKeyComparator, env,
                vals.nextFileNumber + 1, vals.nextFileNumber, vals.lastSequence, vals.logNumber,
                vals.prevLogNumber, vals.versionBuilder.build(), vals.compactedPointers));
      });
    });
  }

  private static CompletionStage<String> readStringFromFile(final Env env,
      final FileInfo fileInfo) {
    return env.openSequentialReadFile(fileInfo).thenCompose(reader -> CompletableFutures
        .closeAfter(buildString(reader, new StringBuilder(), ByteBuffer.allocate(1024)), reader));
  }

  private static CompletionStage<String> buildString(final SequentialReadFile reader,
      final StringBuilder builder, final ByteBuffer buffer) {
    return reader.read(buffer).thenCompose(bytesRead -> {
      if (bytesRead > 0) {
        buffer.flip();
        assert buffer.hasArray();
        builder.append(new String(buffer.array(), buffer.position(), buffer.remaining(),
            StandardCharsets.UTF_8));
        buffer.clear();
        return buildString(reader, builder, buffer);
      } else {
        return CompletableFuture.completedFuture(builder.toString());
      }
    });
  }

  @Override
  public CompletionStage<Void> asyncClose() {
    current.set(null);
    return Closeables.asyncClose(descriptorLog);
  }

  private void appendVersion(final Version previous, final Version update) {
    final boolean newEntry = activeVersions.add(update);
    assert newEntry : "appended already existing version";
    if (!current.compareAndSet(previous, update)) {
      throw new IllegalStateException("unexpected interleaved version update");
    }
  }

  public Version getCurrent() {
    return current.get();
  }

  public long getManifestFileNumber() {
    return manifestFileNumber;
  }

  // TODO check this
  /**
   * must be called from compaction chain
   */
  public long getAndIncrementNextFileNumber() {
    return nextFileNumber.getAndIncrement();
  }

  public long getLogNumber() {
    return logNumber;
  }

  public long getPrevLogNumber() {
    return prevLogNumber;
  }

  public CompletionStage<LookupResult> get(final LookupKey key) {
    return getCurrent().get(key);
  }

  public long getLastSequence() {
    return lastSequence.get();
  }

  public long getAndAddLastSequence(final long delta) {
    return lastSequence.getAndAdd(delta);
  }

  public CompletionStage<Void> logAndApply(final VersionEdit edit) {
    // TODO measure/observe contention, maybe coalesce edits
    final CompletableFuture<Void> attempt = new CompletableFuture<>();
    logAndApplyStep(edit, attempt);
    return attempt;
  }

  private void logAndApplyStep(final VersionEdit edit, final CompletableFuture<Void> attempt) {
    final CompletionStage<Void> last = logAndApplyMutex.get();
    last.whenComplete((success, exception) -> {
      if (logAndApplyMutex.compareAndSet(last, attempt)) {
        // successful acquire
        CompletableFutures.compose(logAndApplyInternal(edit), attempt);
      } else {
        // someone else acquired, recurse and re-read mutex
        logAndApplyStep(edit, attempt);
      }
    });
  }

  private CompletionStage<Void> logAndApplyInternal(final VersionEdit edit) {
    final long nextFileNum = nextFileNumber.get();
    final long lastSequenceNum = lastSequence.get();
    final Version base = getCurrent();

    if (edit.getLogNumber().isPresent()) {
      final long num = edit.getLogNumber().getAsLong();
      Preconditions.checkArgument(num >= logNumber);
      Preconditions.checkArgument(num < nextFileNum);
    } else {
      edit.setLogNumber(logNumber);
    }

    if (!edit.getPreviousLogNumber().isPresent()) {
      edit.setPreviousLogNumber(prevLogNumber);
    }

    edit.setNextFileNumber(nextFileNum);
    edit.setLastSequenceNumber(lastSequenceNum);

    // Update compaction pointers
    VersionEdit.mergeCompactPointers(edit.getCompactPointers(), compactPointers);

    // TODO cycle manifest when size exceeds threshold
    final CompletionStage<Void> manifestWrite;
    // Initialize new descriptor log file if necessary by creating
    // a temporary file that contains a snapshot of the current version.
    if (descriptorLog == null) {
      final FileInfo manifestFile = FileInfo.manifest(dbHandle, manifestFileNumber);
      manifestWrite =
          Logs.createLogWriter(manifestFile, env).thenCompose(logWriter -> CompletableFutures
              .composeOnException(writeSnapshot(descriptorLog = logWriter, base), exception -> {
                descriptorLog = null;
                return logWriter.asyncClose().thenCompose(voided -> env.deleteFile(manifestFile));
              }).thenCompose(voided -> writeEditToLog(edit, descriptorLog))
              .thenCompose(voided -> setCurrentFile(env, dbHandle, descriptorLog.getFileNumber())));
    } else {
      manifestWrite = writeEditToLog(edit, descriptorLog);
    }

    final Version version =
        new VersionBuilder(internalKeyComparator, tableCache, base.getFiles()).apply(edit).build();

    // Install the new version
    return manifestWrite.thenAccept(vodied -> {
      appendVersion(base, version);
      logNumber = edit.getLogNumber().orElseThrow(IllegalStateException::new);
      prevLogNumber = edit.getPreviousLogNumber().orElseThrow(IllegalStateException::new);
    });
  }

  private CompletionStage<Void> writeSnapshot(final LogWriter log, final Version base) {
    // Save metadata
    final VersionEdit edit = new VersionEdit();
    edit.setComparatorName(internalKeyComparator.getUserComparator().name());

    // Save compaction pointers
    edit.setCompactPointers(compactPointers);

    // Save files
    edit.addFiles(base.getFiles());

    return writeEditToLog(edit, log);
  }

  private static CompletionStage<Void> writeEditToLog(final VersionEdit edit, final LogWriter log) {
    final GrowingBuffer record = edit.encode(MemoryManagers.heap());
    return log.addRecord(record.get(), true).whenComplete((success, exception) -> record.close());
  }

  /**
   * Make the CURRENT file point to the descriptor file with the specified number.
   */
  public static CompletionStage<Void> setCurrentFile(final Env env, final DBHandle dbHandle,
      final long descriptorNumber) {
    final FileInfo temp = FileInfo.temp(dbHandle, descriptorNumber);
    return env
        .openTemporaryWriteFile(temp,
            FileInfo
                .current(dbHandle))
        .thenCompose(
            file -> CompletableFutures.composeUnconditionally(CompletableFutures.composeOnException(
                file.write(ByteBuffer.wrap((descriptorStringName(descriptorNumber) + "\n")
                    .getBytes(StandardCharsets.UTF_8))).thenCompose(ignored -> file.sync()),
            writeOrSyncFailure -> env.deleteFile(temp)), voided -> file.asyncClose()));
  }

  private static final String MANIFEST_PREFIX = "MANIFEST-";

  public static String descriptorStringName(final long fileNumber) {
    Preconditions.checkArgument(fileNumber >= 0, "number is negative");
    return String.format(MANIFEST_PREFIX + "%06d", fileNumber);
  }

  public static long descriptorFileNumber(final String stringName) {
    return Long.parseLong(stringName.substring(MANIFEST_PREFIX.length()));
  }

  public Stream<FileMetaData> getLiveFiles() {
    return activeVersions.stream().map(Version::getFiles).flatMap(Arrays::stream)
        .flatMap(Arrays::stream);
  }

  private static double maxBytesForLevel(int level) {
    // Note: the result for level zero is not really used since we set
    // the level-0 compaction threshold based on number of files.
    double result = 10 * 1048576.0; // Result for both level-0 and level-1
    while (level > 1) {
      result *= 10;
      level--;
    }
    return result;
  }

  public static long maxFileSizeForLevel(@SuppressWarnings("unused") final int level) {
    return DbConstants.TARGET_FILE_SIZE; // We could vary per level to reduce number of files?
  }

  public boolean needsCompaction() {
    final Version v = current.get();
    return v.getCompactionScore() >= 1 || v.getSeekCompaction() != null;
  }

  public Compaction compactRange(final int level, final InternalKey begin, final InternalKey end) {
    final Version v = current.get();
    final List<FileMetaData> levelInputs = v.getOverlappingInputs(level, begin, end);
    if (levelInputs.isEmpty()) {
      return null;
    }

    return setupOtherInputs(v, level, levelInputs);
  }

  public Compaction pickCompaction() {
    final Version v = current.get();
    // We prefer compactions triggered by too much data in a level over
    // the compactions triggered by seeks.
    final boolean shouldSizeCompact = (v.getCompactionScore() >= 1);
    final Entry<FileMetaData, Integer> seekCompaction;

    int level;
    FileMetaData inputFile = null;
    if (shouldSizeCompact) {
      level = v.getCompactionLevel();
      assert (level >= 0);
      assert (level + 1 < NUM_LEVELS);
      // Pick the first file that comes after compact_pointer_[level]

      if (compactPointers[level] == null) {
        inputFile = v.getFiles()[level][0];
      } else {
        // TODO(optimization) this could be a binary search for levels > 0
        for (final FileMetaData fileMetaData : v.getFiles(level)) {
          if (internalKeyComparator.compare(fileMetaData.getLargest(),
              compactPointers[level]) > 0) {
            inputFile = fileMetaData;
            break;
          }
        }
        if (inputFile == null) {
          inputFile = v.getFiles()[level][0];
        }
      }
    } else if ((seekCompaction = v.getSeekCompaction()) != null) {
      level = seekCompaction.getValue();
      inputFile = seekCompaction.getKey();
    } else {
      return null;
    }

    // Files in level 0 may overlap each other, so pick up all overlapping ones
    final List<FileMetaData> levelInputs =
        level == 0 ? v.getOverlappingInputs(0, inputFile.getSmallest(), inputFile.getLargest())
            : Collections.singletonList(inputFile);

    return setupOtherInputs(v, level, levelInputs);
  }

  Compaction setupOtherInputs(final Version v, final int level, List<FileMetaData> levelInputs) {
    Entry<InternalKey, InternalKey> range = getRange(levelInputs);
    InternalKey smallest = range.getKey();
    InternalKey largest = range.getValue();

    List<FileMetaData> levelUpInputs = v.getOverlappingInputs(level + 1, smallest, largest);

    // Get entire range covered by compaction
    range = getRange(levelInputs, levelUpInputs);
    InternalKey allStart = range.getKey();
    InternalKey allLimit = range.getValue();

    // See if we can grow the number of inputs in "level" without
    // changing the number of "level+1" files we pick up.
    if (!levelUpInputs.isEmpty()) {

      final List<FileMetaData> expanded0 = v.getOverlappingInputs(level, allStart, allLimit);

      // TODO kExpandedCompactionByteSizeLimit
      if (expanded0.size() > levelInputs.size()) {
        range = getRange(expanded0);
        final InternalKey newStart = range.getKey();
        final InternalKey newLimit = range.getValue();

        final List<FileMetaData> expanded1 = v.getOverlappingInputs(level + 1, newStart, newLimit);
        if (expanded1.size() == levelUpInputs.size()) {
          LOGGER.debug("Expanding@{} {}+{} to {}+{}", level, levelInputs.size(),
              levelUpInputs.size(), expanded0.size(), expanded1.size());
          smallest = newStart;
          largest = newLimit;
          levelInputs = expanded0;
          levelUpInputs = expanded1;

          range = getRange(levelInputs, levelUpInputs);
          allStart = range.getKey();
          allLimit = range.getValue();
        }
      }
    }

    // Compute the set of grandparent files that overlap this compaction
    // (parent == level+1; grandparent == level+2)
    List<FileMetaData> grandparents = null;
    if (level + 2 < NUM_LEVELS) {
      grandparents = v.getOverlappingInputs(level + 2, allStart, allLimit);
    }

    LOGGER.trace("Compacting level {} '{}' .. '{}'", level, smallest, largest);

    final Compaction compaction =
        new Compaction(v, level, levelInputs, levelUpInputs, grandparents);

    // Update the place where we will do the next compaction for this level.
    // We update this immediately instead of waiting for the VersionEdit
    // to be applied so that if the compaction fails, we will try a different
    // key range next time.
    compactPointers[level] = largest;
    compaction.getEdit().setCompactPointer(level, largest);

    return compaction;
  }

  private final Entry<InternalKey, InternalKey> getRange(final List<FileMetaData> files) {
    return getRange(files.iterator());
  }

  private final Entry<InternalKey, InternalKey> getRange(final List<FileMetaData> f1,
      final List<FileMetaData> f2) {
    return getRange(Iterators.concat(f1.iterator(), f2.iterator()));
  }

  private final Entry<InternalKey, InternalKey> getRange(final Iterator<FileMetaData> files) {
    assert files.hasNext();
    final FileMetaData first = files.next();
    InternalKey smallest = first.getSmallest();
    InternalKey largest = first.getLargest();

    while (files.hasNext()) {
      final FileMetaData fileMetaData = files.next();

      if (internalKeyComparator.compare(fileMetaData.getSmallest(), smallest) < 0) {
        smallest = fileMetaData.getSmallest();
      }
      if (internalKeyComparator.compare(fileMetaData.getLargest(), largest) > 0) {
        largest = fileMetaData.getLargest();
      }
    }
    return Maps.immutableEntry(smallest, largest);
  }

  public CompletionStage<SeekingAsynchronousIterator<InternalKey, ByteBuffer>> makeInputIterator(
      final Compaction c) {
    // Level-0 files have to be merged together. For other levels,
    // we will make a concatenating iterator per level.
    // TODO(opt): use concatenating iterator for level-0 if there is no overlap
    CompletionStage<SeekingAsynchronousIterator<InternalKey, ByteBuffer>> level0Iter = null;
    final ImmutableList.Builder<SeekingAsynchronousIterator<InternalKey, ByteBuffer>> list =
        ImmutableList.builder();
    for (int which = 0; which < 2; which++) {
      final List<FileMetaData> input = c.input(which);
      if (!input.isEmpty()) {
        if (c.getLevel() + which == 0) {
          level0Iter = Version.newLevel0Iterator(input.stream(), tableCache, internalKeyComparator);
        } else {
          // Create concatenating iterator for the files from this level
          list.add(new Version.LevelIterator(tableCache,
              input.toArray(new FileMetaData[input.size()]), internalKeyComparator));
        }
      }
    }
    return level0Iter != null
        ? level0Iter.thenApply(level0 -> MergingIterator
            .newMergingIterator(list.add(level0).build(), internalKeyComparator))
        : CompletableFuture.completedFuture(
            MergingIterator.newMergingIterator(list.build(), internalKeyComparator));
  }

  public long getMaxNextLevelOverlappingBytes() {
    final Version v = current.get();
    long result = 0;
    for (int level = 1; level < NUM_LEVELS; level++) {
      for (final FileMetaData fileMetaData : v.getFiles(level)) {
        final List<FileMetaData> overlaps = v.getOverlappingInputs(level + 1,
            fileMetaData.getSmallest(), fileMetaData.getLargest());
        long totalSize = 0;
        for (final FileMetaData overlap : overlaps) {
          totalSize += overlap.getFileSize();
        }
        result = Math.max(result, totalSize);
      }
    }
    return result;
  }

  /**
   * A helper class so we can efficiently apply a whole sequence of edits to a particular state
   * without creating intermediate Versions that contain full copies of the intermediate state.
   */
  private static class VersionBuilder {
    private final InternalKeyComparator internalKeyComparator;
    private final TableCache tableCache;
    private final FileMetaData[][] baseFiles;
    private final LevelState[] levels = new LevelState[NUM_LEVELS];
    private static final FileMetaData[] EMPTY_LEVEL = new FileMetaData[0];

    private VersionBuilder(final InternalKeyComparator internalKeyComparator,
        final TableCache tableCache, final FileMetaData[][] baseFiles) {
      this.internalKeyComparator = internalKeyComparator;
      this.tableCache = tableCache;
      this.baseFiles = baseFiles;

      for (int i = 0; i < NUM_LEVELS; i++) {
        levels[i] =
            new LevelState(internalKeyComparator, i < baseFiles.length ? baseFiles[i].length : 0);
      }
    }

    /**
     * Apply the specified edit to the current state.
     */
    public VersionBuilder apply(final VersionEdit edit) {

      {
        // Delete files
        int level = 0;
        for (final List<Long> deletions : edit.getDeletedFiles()) {
          levels[level++].deletedFiles.addAll(deletions);
        }
      }

      {
        // Add new files
        int level = 0;
        for (final List<FileMetaData> newFiles : edit.getNewFiles()) {
          for (final FileMetaData fileMetaData : newFiles) {
            // We arrange to automatically compact this file after
            // a certain number of seeks. Let's assume:
            // (1) One seek costs 10ms
            // (2) Writing or reading 1MB costs 10ms (100MB/s)
            // (3) A compaction of 1MB does 25MB of IO:
            // 1MB read from this level
            // 10-12MB read from next level (boundaries may be misaligned)
            // 10-12MB written to next level
            // This implies that 25 seeks cost the same as the compaction
            // of 1MB of data. I.e., one seek costs approximately the
            // same as the compaction of 40KB of data. We are a little
            // conservative and allow approximately one seek for every 16KB
            // of data before triggering a compaction.
            int allowedSeeks = (int) (fileMetaData.getFileSize() / 16384);
            if (allowedSeeks < 100) {
              allowedSeeks = 100;
            }
            fileMetaData.setAllowedSeeks(allowedSeeks);

            levels[level].deletedFiles.remove(fileMetaData.getNumber());
            levels[level].addedFiles.add(fileMetaData);
          }
          level++;
        }
      }

      return this;
    }

    /**
     * Saves the current state in specified version.
     */
    private void collectFiles() {
      final FileMetaDataBySmallestKey cmp = new FileMetaDataBySmallestKey(internalKeyComparator);
      for (int level = 0; level < NUM_LEVELS; level++) {

        // Merge the set of added files with the set of pre-existing files.
        // Drop any deleted files

        final FileMetaData[] levelFiles = level < baseFiles.length ? baseFiles[level] : EMPTY_LEVEL;
        int lfile = 0;
        // files must be added in sorted order so assertion check in maybeAddFile works
        for (final FileMetaData addedFile : levels[level].addedFiles) {
          while (lfile < levelFiles.length && cmp.compare(levelFiles[lfile], addedFile) <= 0) {
            maybeAddFile(level, levelFiles[lfile++]);
          }
          maybeAddFile(level, addedFile);
        }
      }
    }

    private void maybeAddFile(final int level, final FileMetaData fileMetaData) {
      if (levels[level].deletedFiles.contains(fileMetaData.getNumber())) {
        // File is deleted: do nothing
      } else {
        final List<FileMetaData> files = levels[level].collectedFiles;
        if (level > 0 && !files.isEmpty()) {
          // Must not overlap
          final boolean filesOverlap = internalKeyComparator
              .compare(files.get(files.size() - 1).getLargest(), fileMetaData.getSmallest()) >= 0;
          if (filesOverlap) {
            // A memory compaction, while this compaction was running, resulted in a a database
            // state that is
            // incompatible with the compaction. This is rare and expensive to detect while the
            // compaction is
            // running, so we catch here simply discard the work.
            // TODO check this?
            throw new IllegalStateException(
                String.format("Compaction is obsolete: Overlapping files %s and %s in level %s",
                    files.get(files.size() - 1).getNumber(), fileMetaData.getNumber(), level));
          }
        }
        levels[level].collectedFiles.add(fileMetaData);
      }
    }

    public Version build() {
      collectFiles();
      // Precomputed best level for next compaction
      int bestLevel = -1;
      double bestScore = -1;

      for (int level = 0; level < levels.length - 1; level++) {
        double score;
        if (level == 0) {
          // We treat level-0 specially by bounding the number of files
          // instead of number of bytes for two reasons:
          //
          // (1) With larger write-buffer sizes, it is nice not to do too
          // many level-0 compactions.
          //
          // (2) The files in level-0 are merged on every read and
          // therefore we wish to avoid too many files when the individual
          // file size is small (perhaps because of a small write-buffer
          // setting, or very high compression ratios, or lots of
          // overwrites/deletions).
          score = 1.0 * levels[level].collectedFiles.size() / L0_COMPACTION_TRIGGER;
        } else {
          // Compute the ratio of current size to size limit.
          long levelBytes = 0;
          for (final FileMetaData fileMetaData : levels[level].collectedFiles) {
            levelBytes += fileMetaData.getFileSize();
          }
          score = 1.0 * levelBytes / maxBytesForLevel(level);
        }

        if (score > bestScore) {
          bestLevel = level;
          bestScore = score;
        }
      }

      final FileMetaData[][] levelFiles =
          Stream.of(levels)
              .map(level -> level.collectedFiles
                  .toArray(new FileMetaData[level.collectedFiles.size()]))
          .toArray(FileMetaData[][]::new);
      final Version version =
          new Version(levelFiles, bestLevel, bestScore, tableCache, internalKeyComparator);
      // Make sure there is no overlap in levels > 0
      assert version.assertNoOverlappingFiles();
      return version;
    }

    private static class FileMetaDataBySmallestKey implements Comparator<FileMetaData> {
      private final InternalKeyComparator internalKeyComparator;

      private FileMetaDataBySmallestKey(final InternalKeyComparator internalKeyComparator) {
        this.internalKeyComparator = internalKeyComparator;
      }

      @Override
      public int compare(final FileMetaData f1, final FileMetaData f2) {
        return ComparisonChain.start()
            .compare(f1.getSmallest(), f2.getSmallest(), internalKeyComparator)
            .compare(f1.getNumber(), f2.getNumber()).result();
      }
    }

    private static class LevelState {
      private final SortedSet<FileMetaData> addedFiles;
      private final Set<Long> deletedFiles = new HashSet<Long>();
      private final List<FileMetaData> collectedFiles;

      public LevelState(final InternalKeyComparator internalKeyComparator,
          final int fileCountEstimate) {
        this.addedFiles =
            new TreeSet<FileMetaData>(new FileMetaDataBySmallestKey(internalKeyComparator));
        this.collectedFiles = new ArrayList<>(fileCountEstimate);
      }

      @Override
      public String toString() {
        return "LevelState [addedFiles=" + addedFiles + ", deletedFiles=" + deletedFiles + "]";
      }
    }
  }
}
