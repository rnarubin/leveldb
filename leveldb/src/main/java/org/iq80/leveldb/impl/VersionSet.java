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
package org.iq80.leveldb.impl;

import static org.iq80.leveldb.impl.DbConstants.NUM_LEVELS;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.iq80.leveldb.AsynchronousCloseable;
import org.iq80.leveldb.Env;
import org.iq80.leveldb.Env.DBHandle;
import org.iq80.leveldb.Env.SequentialReadFile;
import org.iq80.leveldb.FileInfo;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.SeekingAsynchronousIterator;
import org.iq80.leveldb.util.Closeables;
import org.iq80.leveldb.util.CompletableFutures;
import org.iq80.leveldb.util.GrowingBuffer;
import org.iq80.leveldb.util.MemoryManagers;
import org.iq80.leveldb.util.MergingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.PeekingIterator;

public class VersionSet implements AsynchronousCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(VersionSet.class);

  private static final int L0_COMPACTION_TRIGGER = 4;

  public static final int TARGET_FILE_SIZE = 2 * 1048576;

  // Maximum bytes of overlaps in grandparent (i.e., level+2) before we
  // stop building a single file in a level.level+1 compaction.
  public static final long MAX_GRAND_PARENT_OVERLAP_BYTES = 10 * TARGET_FILE_SIZE;

  private final AtomicLong nextFileNumber = new AtomicLong(2);
  private long manifestFileNumber = 1;
  private volatile Version current;
  private final AtomicLong lastSequence = new AtomicLong(0);
  private long logNumber;
  private long prevLogNumber;

  private static final Object PRESENT = new Object();
  private final Map<Version, Object> activeVersions = new MapMaker().weakKeys().makeMap();
  private final DBHandle dbHandle;
  private final TableCache tableCache;
  private final InternalKeyComparator internalKeyComparator;
  private final Options options;

  private LogWriter descriptorLog;
  private final Map<Integer, InternalKey> compactPointers = Maps.newTreeMap();

  private VersionSet(final DBHandle dbHandle, final TableCache tableCache,
      final InternalKeyComparator internalKeyComparator, final Options options) {
    this.dbHandle = dbHandle;
    this.tableCache = tableCache;
    this.internalKeyComparator = internalKeyComparator;
    this.options = options;
    appendVersion(new Version());
  }

  public static CompletionStage<VersionSet> newVersionSet(final DBHandle dbHandle,
      final TableCache tableCache, final InternalKeyComparator internalKeyComparator,
      final Options options) {
    return options.env().fileExists(FileInfo.current(dbHandle)).thenCompose(exists -> {
      if (exists) {
        return CompletableFuture.<Void>completedFuture(null);
      } else {
        // create new initial manifest file
        final VersionEdit edit = new VersionEdit();
        edit.setComparatorName(internalKeyComparator.getUserComparator().name());
        edit.setLastSequenceNumber(0L);
        edit.setLogNumber(0L);
        final long manifestNum = 1L;
        edit.setNextFileNumber(2L);
        final GrowingBuffer record = edit.encode(MemoryManagers.heap());

        return Logs.createLogWriter(FileInfo.manifest(dbHandle, manifestNum), manifestNum, options)
            .thenCompose(logWriter -> CompletableFutures.composeUnconditionally(
                logWriter.addRecord(record.get(), true),
                voided -> logWriter.asyncClose().<Void>handle((close, closeException) -> {
          record.close();
          return null;
        }))).thenCompose(voided -> setCurrentFile(options.env(), dbHandle, manifestNum));
      }
    }).thenApply(voided -> new VersionSet(dbHandle, tableCache, internalKeyComparator, options));
  }

  @Override
  public CompletionStage<Void> asyncClose() {
    current = null;
    return Closeables.asyncClose(descriptorLog);
  }

  // TODO check synchronization
  private void appendVersion(final Version version) {
    final Object oldMapping = activeVersions.put(version, PRESENT);
    assert oldMapping == null : "appended already existing version";
    current = version;
  }

  public InternalKeyComparator getInternalKeyComparator() {
    return internalKeyComparator;
  }

  public TableCache getTableCache() {
    return tableCache;
  }

  public Version getCurrent() {
    return current;
  }

  public long getManifestFileNumber() {
    return manifestFileNumber;
  }

  public long getNextFileNumber() {
    return nextFileNumber.getAndIncrement();
  }

  public long getLogNumber() {
    return logNumber;
  }

  public long getPrevLogNumber() {
    return prevLogNumber;
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
      final List<FileMetaData> input = c.getInputs()[which];
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

  public CompletionStage<LookupResult> get(final LookupKey key) {
    return current.get(key);
  }

  public int numberOfFilesInLevel(final int level) {
    return current.numberOfFilesInLevel(level);
  }

  public long numberOfBytesInLevel(final int level) {
    return current.numberOfFilesInLevel(level);
  }

  public long getLastSequence() {
    return lastSequence.get();
  }

  public long getAndAddLastSequence(final long delta) {
    return lastSequence.getAndAdd(delta);
  }

  public void setLastSequence(final long newLastSequence) {
    Preconditions.checkArgument(newLastSequence >= lastSequence.get(),
        "Expected newLastSequence to be greater than or equal to current lastSequence");
    this.lastSequence.set(newLastSequence);
  }

  public void logAndApply(final VersionEdit edit) throws IOException {
    if (edit.getLogNumber() != null) {
      Preconditions.checkArgument(edit.getLogNumber() >= logNumber);
      Preconditions.checkArgument(edit.getLogNumber() < nextFileNumber.get());
    } else {
      edit.setLogNumber(logNumber);
    }

    if (edit.getPreviousLogNumber() == null) {
      edit.setPreviousLogNumber(prevLogNumber);
    }

    edit.setNextFileNumber(nextFileNumber.get());
    edit.setLastSequenceNumber(lastSequence.get());

    // Update compaction pointers
    for (final Entry<Integer, InternalKey> entry : edit.getCompactPointers().entrySet()) {
      final Integer level = entry.getKey();
      final InternalKey internalKey = entry.getValue();
      compactPointers.put(level, internalKey);
    }
    final VersionBuilder builder = new VersionBuilder(this, current);
    final Version version = builder.apply(edit).build();

    boolean createdNewManifest = false;
    final FileInfo manifestFile = FileInfo.manifest(dbHandle, manifestFileNumber);
    try {
      // Initialize new descriptor log file if necessary by creating
      // a temporary file that contains a snapshot of the current version.
      if (descriptorLog == null) {
        edit.setNextFileNumber(nextFileNumber.get());
        descriptorLog = Logs.createLogWriter(manifestFile, manifestFileNumber, options);
        writeSnapshot(descriptorLog);
        createdNewManifest = true;
      }

      // Write new record to MANIFEST log
      try (GrowingBuffer record = edit.encode(options.memoryManager())) {
        descriptorLog.addRecord(record.get(), true);
      }

      // If we just created a new descriptor file, install it by writing a
      // new CURRENT file that points to it.
      if (createdNewManifest) {
        setCurrentFile(options.env(), descriptorLog.getFileNumber());
      }
    } catch (final IOException e) {
      // New manifest file was not installed, so clean up state and delete the file
      if (createdNewManifest) {
        descriptorLog.close();
        options.env().deleteFile(manifestFile);
        descriptorLog = null;
      }
      throw e;
    }

    // Install the new version
    appendVersion(version);
    logNumber = edit.getLogNumber();
    prevLogNumber = edit.getPreviousLogNumber();
  }

  private static CompletionStage<Void> writeSnapshot(final LogWriter log,
      final InternalKeyComparator internalKeyComparator,
      final Map<Integer, InternalKey> compactPointers,
      final Multimap<Integer, FileMetaData> currentFiles) throws IOException {
    // Save metadata
    final VersionEdit edit = new VersionEdit();
    edit.setComparatorName(internalKeyComparator.name());

    // Save compaction pointers
    edit.setCompactPointers(compactPointers);

    // Save files
    edit.addFiles(currentFiles);

    // TODO this try is wrong
    try (GrowingBuffer record = edit.encode(MemoryManagers.heap())) {
      log.addRecord(record.get(), true);
    }
  }

  public void recover() throws IOException {

    // Read "CURRENT" file, which contains a pointer to the current manifest file
    final FileInfo currentFile = FileInfo.current(dbHandle);
    Preconditions.checkState(options.env().fileExists(currentFile), "CURRENT file does not exist");

    String currentName = readStringFromFile(options.env(), currentFile);
    if (currentName.isEmpty() || currentName.charAt(currentName.length() - 1) != '\n') {
      throw new IllegalStateException("CURRENT file does not end with newline");
    }
    currentName = currentName.substring(0, currentName.length() - 1);

    // open file channel
    try (
        SequentialReadFile fileInput = options.env()
            .openSequentialReadFile(FileInfo.manifest(dbHandle, descriptorFileNumber(currentName)));
        LogReader reader =
            new LogReader(fileInput, throwExceptionMonitor(), true, 0, options.memoryManager())) {
      // read log edit log
      Long nextFileNumber = null;
      Long lastSequence = null;
      Long logNumber = null;
      Long prevLogNumber = null;
      final VersionBuilder builder = new VersionBuilder(this, current);

      for (ByteBuffer record = reader.readRecord(); record != null; record = reader.readRecord()) {
        // read version edit
        final VersionEdit edit = new VersionEdit(record);
        options.memoryManager().free(record);

        // verify comparator
        // todo implement user comparator
        final String editComparator = edit.getComparatorName();
        final String userComparator = internalKeyComparator.name();
        Preconditions.checkArgument(editComparator == null || editComparator.equals(userComparator),
            "Expected user comparator %s to match existing database comparator ", userComparator,
            editComparator);

        // Update compaction pointers
        for (final Entry<Integer, InternalKey> entry : edit.getCompactPointers().entrySet()) {
          final Integer level = entry.getKey();
          final InternalKey internalKey = entry.getValue();
          compactPointers.put(level, internalKey);
        }
        // apply edit
        builder.apply(edit);

        // save edit values for verification below
        logNumber = coalesce(edit.getLogNumber(), logNumber);
        prevLogNumber = coalesce(edit.getPreviousLogNumber(), prevLogNumber);
        nextFileNumber = coalesce(edit.getNextFileNumber(), nextFileNumber);
        lastSequence = coalesce(edit.getLastSequenceNumber(), lastSequence);
      }

      final List<String> problems = newArrayList();
      if (nextFileNumber == null) {
        problems.add("Descriptor does not contain a meta-nextfile entry");
      }
      if (logNumber == null) {
        problems.add("Descriptor does not contain a meta-lognumber entry");
      }
      if (lastSequence == null) {
        problems.add("Descriptor does not contain a last-sequence-number entry");
      }
      if (!problems.isEmpty()) {
        throw new RuntimeException("Corruption: \n\t" + Joiner.on("\n\t").join(problems));
      }

      if (prevLogNumber == null) {
        prevLogNumber = 0L;
      }


      final Version newVersion = builder.build();

      // Install recovered version
      appendVersion(newVersion);
      manifestFileNumber = nextFileNumber;
      this.nextFileNumber.set(nextFileNumber + 1);
      this.lastSequence.set(lastSequence);
      this.logNumber = logNumber;
      this.prevLogNumber = prevLogNumber;
    }
  }

  @SafeVarargs
  private static <V> V coalesce(final V... values) {
    for (final V value : values) {
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  public List<FileMetaData> getLiveFiles() {
    final ImmutableList.Builder<FileMetaData> builder = ImmutableList.builder();
    for (final Version activeVersion : activeVersions.keySet()) {
      builder.addAll(activeVersion.getFiles().values());
    }
    return builder.build();
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

  public static long maxFileSizeForLevel(final int level) {
    return TARGET_FILE_SIZE; // We could vary per level to reduce number of files?
  }

  public boolean needsCompaction() {
    return current.getCompactionScore() >= 1 || current.getFileToCompact() != null;
  }

  public Compaction compactRange(final int level, final InternalKey begin, final InternalKey end) {
    final List<FileMetaData> levelInputs = getOverlappingInputs(level, begin, end);
    if (levelInputs.isEmpty()) {
      return null;
    }

    return setupOtherInputs(level, levelInputs);
  }

  public Compaction pickCompaction() {
    // We prefer compactions triggered by too much data in a level over
    // the compactions triggered by seeks.
    final boolean sizeCompaction = (current.getCompactionScore() >= 1);
    final boolean seekCompaction = (current.getFileToCompact() != null);

    int level;
    List<FileMetaData> levelInputs;
    if (sizeCompaction) {
      level = current.getCompactionLevel();
      Preconditions.checkState(level >= 0);
      Preconditions.checkState(level + 1 < NUM_LEVELS);

      // Pick the first file that comes after compact_pointer_[level]
      levelInputs = newArrayList();
      for (final FileMetaData fileMetaData : current.getFiles(level)) {
        if (!compactPointers.containsKey(level) || internalKeyComparator
            .compare(fileMetaData.getLargest(), compactPointers.get(level)) > 0) {
          levelInputs.add(fileMetaData);
          break;
        }
      }
      if (levelInputs.isEmpty()) {
        // Wrap-around to the beginning of the key space
        levelInputs.add(current.getFiles(level).get(0));
      }
    } else if (seekCompaction) {
      level = current.getFileToCompactLevel();
      levelInputs = ImmutableList.of(current.getFileToCompact());
    } else {
      return null;
    }

    // Files in level 0 may overlap each other, so pick up all overlapping ones
    if (level == 0) {
      final Entry<InternalKey, InternalKey> range = getRange(levelInputs);
      // Note that the next call will discard the file we placed in
      // c->inputs_[0] earlier and replace it with an overlapping set
      // which will include the picked file.
      levelInputs = getOverlappingInputs(0, range.getKey(), range.getValue());

      Preconditions.checkState(!levelInputs.isEmpty());
    }

    final Compaction compaction = setupOtherInputs(level, levelInputs);
    return compaction;
  }

  private Compaction setupOtherInputs(final int level, List<FileMetaData> levelInputs) {
    Entry<InternalKey, InternalKey> range = getRange(levelInputs);
    InternalKey smallest = range.getKey();
    InternalKey largest = range.getValue();

    List<FileMetaData> levelUpInputs = getOverlappingInputs(level + 1, smallest, largest);

    // Get entire range covered by compaction
    range = getRange(levelInputs, levelUpInputs);
    InternalKey allStart = range.getKey();
    InternalKey allLimit = range.getValue();

    // See if we can grow the number of inputs in "level" without
    // changing the number of "level+1" files we pick up.
    if (!levelUpInputs.isEmpty()) {

      final List<FileMetaData> expanded0 = getOverlappingInputs(level, allStart, allLimit);

      if (expanded0.size() > levelInputs.size()) {
        range = getRange(expanded0);
        final InternalKey newStart = range.getKey();
        final InternalKey newLimit = range.getValue();

        final List<FileMetaData> expanded1 = getOverlappingInputs(level + 1, newStart, newLimit);
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
      grandparents = getOverlappingInputs(level + 2, allStart, allLimit);
    }

    // if (false) {
    // Log(options_ - > info_log, "Compacting %d '%s' .. '%s'",
    // level,
    // EscapeString(smallest.Encode()).c_str(),
    // EscapeString(largest.Encode()).c_str());
    // }

    final Compaction compaction =
        new Compaction(current, level, levelInputs, levelUpInputs, grandparents);

    // Update the place where we will do the next compaction for this level.
    // We update this immediately instead of waiting for the VersionEdit
    // to be applied so that if the compaction fails, we will try a different
    // key range next time.
    compactPointers.put(level, largest);
    compaction.getEdit().setCompactPointer(level, largest);

    return compaction;
  }

  @SafeVarargs
  private final Entry<InternalKey, InternalKey> getRange(final List<FileMetaData>... inputLists) {
    InternalKey smallest = null;
    InternalKey largest = null;
    for (final List<FileMetaData> inputList : inputLists) {
      for (final FileMetaData fileMetaData : inputList) {
        if (smallest == null) {
          smallest = fileMetaData.getSmallest();
          largest = fileMetaData.getLargest();
        } else {
          if (internalKeyComparator.compare(fileMetaData.getSmallest(), smallest) < 0) {
            smallest = fileMetaData.getSmallest();
          }
          if (internalKeyComparator.compare(fileMetaData.getLargest(), largest) > 0) {
            largest = fileMetaData.getLargest();
          }
        }
      }
    }
    return Maps.immutableEntry(smallest, largest);
  }

  public long getMaxNextLevelOverlappingBytes() {
    long result = 0;
    for (int level = 1; level < NUM_LEVELS; level++) {
      for (final FileMetaData fileMetaData : current.getFiles(level)) {
        final List<FileMetaData> overlaps =
            getOverlappingInputs(level + 1, fileMetaData.getSmallest(), fileMetaData.getLargest());
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
   * Make the CURRENT file point to the descriptor file with the specified number.
   */
  public static CompletionStage<Void> setCurrentFile(final Env env, final DBHandle dbHandle,
      final long descriptorNumber) {
    final FileInfo temp = FileInfo.temp(dbHandle, descriptorNumber);
    // @formatter:off
    return env.openTemporaryWriteFile(temp, FileInfo.current(dbHandle))
        .thenCompose(file ->
        CompletableFutures.composeUnconditionally(
            CompletableFutures.composeOnException(
                file.write(ByteBuffer.wrap((descriptorStringName(descriptorNumber) + "\n")
                    .getBytes(StandardCharsets.UTF_8)))
                .thenCompose(ignored -> file.sync()),
              writeOrSyncFailure -> env.deleteFile(temp)),
            voided -> file.asyncClose()));
    // @formatter:on
  }

  private static final String MANIFEST_PREFIX = "MANIFEST-";

  public static String descriptorStringName(final long fileNumber) {
    Preconditions.checkArgument(fileNumber >= 0, "number is negative");
    return String.format(MANIFEST_PREFIX + "%06d", fileNumber);
  }

  public static long descriptorFileNumber(final String stringName) {
    return Long.parseLong(stringName.substring(MANIFEST_PREFIX.length()));
  }

  public static String readStringFromFile(final Env env, final FileInfo fileInfo)
      throws IOException {
    final StringBuilder builder = new StringBuilder();
    // rare enough that it's really not an issue not using the
    // MemoryManager; furthermore, accessing the known underlying array
    // facilitates String conversion
    final ByteBuffer buffer = ByteBuffer.allocate(4096);
    try (SequentialReadFile reader = env.openSequentialReadFile(fileInfo)) {
      while (reader.read(buffer) > 0) {
        buffer.flip();
        builder.append(new String(buffer.array(), buffer.position(), buffer.remaining(),
            StandardCharsets.UTF_8));
        buffer.clear();
      }
    }
    return builder.toString();
  }

  /**
   * A helper class so we can efficiently apply a whole sequence of edits to a particular state
   * without creating intermediate Versions that contain full copies of the intermediate state.
   */
  private static class VersionBuilder {
    private final VersionSet versionSet;
    private final Version baseVersion;
    private final LevelState[] levels = new LevelState[NUM_LEVELS];

    private VersionBuilder(final VersionSet versionSet, final Version baseVersion) {
      this.versionSet = versionSet;
      this.baseVersion = baseVersion;

      for (int i = 0; i < NUM_LEVELS; i++) {
        levels[i] = new LevelState(versionSet.getInternalKeyComparator(),
            baseVersion.numberOfFilesInLevel(i));
      }
    }

    /**
     * Apply the specified edit to the current state.
     */
    public VersionBuilder apply(final VersionEdit edit) {

      // Delete files
      for (final Entry<Integer, Long> entry : edit.getDeletedFiles().entries()) {
        final Integer level = entry.getKey();
        final Long fileNumber = entry.getValue();
        levels[level].deletedFiles.add(fileNumber);
      }

      // Add new files
      for (final Entry<Integer, FileMetaData> entry : edit.getNewFiles().entries()) {
        final Integer level = entry.getKey();
        final FileMetaData fileMetaData = entry.getValue();

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

      return this;
    }

    /**
     * Saves the current state in specified version.
     */
    private void collectFiles() {
      final FileMetaDataBySmallestKey cmp =
          new FileMetaDataBySmallestKey(versionSet.internalKeyComparator);
      for (int level = 0; level < NUM_LEVELS; level++) {

        // Merge the set of added files with the set of pre-existing files.
        // Drop any deleted files

        final PeekingIterator<FileMetaData> baseFiles =
            Iterators.peekingIterator(baseVersion.getFiles(level).iterator());
        // files must be added in sorted order so assertion check in maybeAddFile works
        for (final FileMetaData addedFile : levels[level].addedFiles) {
          while (baseFiles.hasNext() && cmp.compare(baseFiles.peek(), addedFile) <= 0) {
            maybeAddFile(level, baseFiles.next());
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
          final boolean filesOverlap = versionSet.internalKeyComparator
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
      final Version version = new Version(versionSet, levelFiles, bestLevel, bestScore);
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
