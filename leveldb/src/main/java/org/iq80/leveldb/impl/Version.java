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

import static org.iq80.leveldb.impl.DbConstants.MAX_MEM_COMPACT_LEVEL;
import static org.iq80.leveldb.impl.SequenceNumber.MAX_SEQUENCE_NUMBER;
import static org.iq80.leveldb.impl.VersionSet.MAX_GRAND_PARENT_OVERLAP_BYTES;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.iq80.leveldb.SeekingAsynchronousIterator;
import org.iq80.leveldb.table.Table.TableIterator;
import org.iq80.leveldb.util.CompletableFutures;
import org.iq80.leveldb.util.MergingIterator;
import org.iq80.leveldb.util.TwoStageIterator;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

final class Version {
  private final FileMetaData[][] files;
  private final TableCache tableCache;
  private final InternalKeyComparator internalKeyComparator;

  private final int compactionLevel;
  private final double compactionScore;
  private final AtomicReference<Entry<FileMetaData, Integer>> seekCompaction =
      new AtomicReference<>(null);

  Version(final FileMetaData[][] levelFiles, final int compactionLevel,
      final double compactionScore, final TableCache tableCache,
      final InternalKeyComparator internalKeyComparator) {
    this.internalKeyComparator = internalKeyComparator;
    this.compactionLevel = compactionLevel;
    this.compactionScore = compactionScore;
    this.tableCache = tableCache;
    this.files = levelFiles;
  }

  /**
   * @return true iff no overlap
   */
  public boolean assertNoOverlappingFiles() {
    for (int level = 1; level < files.length; level++) {
      assertNoOverlappingFiles(level);
    }
    return true;
  }

  private void assertNoOverlappingFiles(final int level) {
    for (int i = 1; i < files[level].length; i++) {
      Preconditions.checkArgument(
          internalKeyComparator.compare(files[level][i - 1].getLargest(),
              files[level][i].getSmallest()) < 0,
          "Overlapping files %s and %s in level %s", files[level][i - 1].getNumber(),
          files[level][i].getNumber(), level);
    }
  }

  public int getCompactionLevel() {
    return compactionLevel;
  }

  public double getCompactionScore() {
    return compactionScore;
  }

  public static CompletionStage<SeekingAsynchronousIterator<InternalKey, ByteBuffer>> newLevel0Iterator(
      final Stream<FileMetaData> files, final TableCache tableCache,
      final Comparator<InternalKey> internalKeyComparator) {
    return CompletableFutures.allOf(files.map(tableCache::tableIterator))
        .thenApply(tableIters -> MergingIterator
            .newMergingIterator(tableIters.collect(Collectors.toList()), internalKeyComparator));
  }

  public CompletionStage<Collection<SeekingAsynchronousIterator<InternalKey, ByteBuffer>>> getIterators() {
    final CompletionStage<SeekingAsynchronousIterator<InternalKey, ByteBuffer>> level0 =
        newLevel0Iterator(Arrays.stream(files[0]), tableCache, internalKeyComparator);
    final ImmutableList.Builder<SeekingAsynchronousIterator<InternalKey, ByteBuffer>> listBuilder =
        ImmutableList.builder();
    for (int i = 1; i < files.length; i++) {
      listBuilder.add(new LevelIterator(tableCache, files[i], internalKeyComparator));
    }
    return level0.thenApply(level0Iter -> listBuilder.add(level0Iter).build());
  }

  public CompletionStage<LookupResult> get(final LookupKey key) {
    // We can search level-by-level since entries never hop across
    // levels. Therefore we are guaranteed that if we find data
    // in an smaller level, later levels are irrelevant.
    final ReadStats readStats = new ReadStats();
    // TODO read sampling, maybe compaction
    return get(key, 0, readStats).whenComplete(
        (result, exception) -> updateStats(readStats.getSeekFileLevel(), readStats.getSeekFile()));
  }

  private CompletionStage<LookupResult> get(final LookupKey key, final int level,
      final ReadStats readStats) {
    if (level == files.length) {
      return CompletableFuture.completedFuture(null);
    }
    if (files[level].length == 0) {
      return get(key, level + 1, readStats);
    }

    final List<FileMetaData> fileMetaDataList;
    if (level == 0) {
      fileMetaDataList = new ArrayList<>();
      for (final FileMetaData fileMetaData : files[level]) {
        if (internalKeyComparator.getUserComparator().compare(key.getUserKey(),
            fileMetaData.getSmallest().getUserKey()) >= 0
            && internalKeyComparator.getUserComparator().compare(key.getUserKey(),
                fileMetaData.getLargest().getUserKey()) <= 0) {
          fileMetaDataList.add(fileMetaData);
        }
      }

      if (fileMetaDataList.isEmpty()) {
        return get(key, level + 1, readStats);
      }

      Collections.sort(fileMetaDataList,
          (file1, file2) -> Long.compare(file2.getNumber(), file1.getNumber()));
    } else {
      // Binary search to find earliest index whose largest key >= ikey.
      final int index = findFile(files[level], key.getInternalKey(), internalKeyComparator);

      // did we find any files that could contain the key?
      if (index >= files[level].length) {
        return get(key, level + 1, readStats);
      }

      // check if the smallest user key in the file is less than the target user key
      final FileMetaData fileMetaData = files[level][index];
      if (internalKeyComparator.getUserComparator().compare(key.getUserKey(),
          fileMetaData.getSmallest().getUserKey()) < 0) {
        return get(key, level + 1, readStats);
      }

      // search this file
      fileMetaDataList = Collections.singletonList(fileMetaData);
    }

    readStats.clear();
    final CompletionStage<LookupResult> levelLookup =
        lookup(key, level, fileMetaDataList.iterator(), null, readStats);

    return levelLookup.thenCompose(
        lookupResult -> lookupResult != null ? levelLookup : get(key, level + 1, readStats));
  }

  private CompletionStage<LookupResult> lookup(final LookupKey key, final int level,
      final Iterator<FileMetaData> fileIter, final FileMetaData prevFile,
      final ReadStats readStats) {
    if (!fileIter.hasNext()) {
      return CompletableFuture.completedFuture(null);
    }
    final FileMetaData file = fileIter.next();
    final CompletionStage<LookupResult> lookup =
        tableCache.tableIterator(file).thenCompose(tableIter -> {
          if (prevFile != null && readStats.getSeekFile() == null) {
            readStats.setSeekFile(file);
            readStats.setSeekFileLevel(level);
          }
          return CompletableFutures.composeUnconditionally(tableIter.seek(key.getInternalKey())
              .thenCompose(voided -> tableIter.next()).thenApply(optNext -> {
            if (optNext.isPresent()) {
              final InternalKey internalKey = optNext.get().getKey();
              if (internalKeyComparator.getUserComparator().compare(key.getUserKey(),
                  internalKey.getUserKey()) == 0) {
                if (internalKey.getValueType() == ValueType.DELETION) {
                  return LookupResult.deleted(key);
                } else {
                  return LookupResult.ok(key, optNext.get().getValue());
                }
              } else {
                return null;
              }
            } else {
              return null;
            }
            // TODO(optimization) overlap async close with next lookup
          }), optLookup -> tableIter.asyncClose().thenApply(voided -> optLookup.orElse(null)));
        });

    return lookup.thenCompose(lookupResult -> lookupResult != null ? lookup
        : lookup(key, level, fileIter, file, readStats));
  }

  private boolean someFileOverlapsRange(final FileMetaData[] files,
      final ByteBuffer smallestUserKey, final ByteBuffer largestUserKey) {
    final InternalKey smallestInternalKey =
        new TransientInternalKey(smallestUserKey, MAX_SEQUENCE_NUMBER, ValueType.VALUE);
    final int index = findFile(files, smallestInternalKey, internalKeyComparator);

    return ((index < files.length) && internalKeyComparator.getUserComparator()
        .compare(largestUserKey, files[index].getSmallest().getUserKey()) >= 0);
  }

  private static int findFile(final FileMetaData[] files, final InternalKey targetKey,
      final Comparator<InternalKey> internalKeyComparator) {
    if (files.length == 0) {
      return 0;
    }

    int left = 0;
    int right = files.length - 1;

    // binary search restart positions to find the restart position immediately before the targetKey
    while (left < right) {
      final int mid = (left + right) / 2;

      if (internalKeyComparator.compare(files[mid].getLargest(), targetKey) < 0) {
        // Key at "mid.largest" is < "target". Therefore all
        // files at or before "mid" are uninteresting.
        left = mid + 1;
      } else {
        // Key at "mid.largest" is >= "target". Therefore all files
        // after "mid" are uninteresting.
        right = mid;
      }
    }
    return right;
  }

  int pickLevelForMemTableOutput(final ByteBuffer smallestUserKey,
      final ByteBuffer largestUserKey) {
    int level = 0;
    if (!someFileOverlapsRange(files[level], smallestUserKey, largestUserKey)) {
      // Push to next level if there is no overlap in next level,
      // and the #bytes overlapping in the level after that are limited.
      final InternalKey start =
          new TransientInternalKey(smallestUserKey, MAX_SEQUENCE_NUMBER, ValueType.VALUE);
      final InternalKey limit = new TransientInternalKey(largestUserKey, 0, ValueType.VALUE);
      while (level < MAX_MEM_COMPACT_LEVEL) {
        if (someFileOverlapsRange(files[level + 1], smallestUserKey, largestUserKey)) {
          break;
        }
        final long sum = Compaction.totalFileSize(getOverlappingInputs(level + 2, start, limit));
        if (sum > MAX_GRAND_PARENT_OVERLAP_BYTES) {
          break;
        }
        level++;
      }
    }
    return level;
  }

  public List<FileMetaData> getOverlappingInputs(final int level, final InternalKey begin,
      final InternalKey end) {
    final List<FileMetaData> inputs = new ArrayList<>();
    ByteBuffer userBegin = begin.getUserKey();
    ByteBuffer userEnd = end.getUserKey();
    final Comparator<ByteBuffer> userComparator = internalKeyComparator.getUserComparator();
    for (int i = 0; i < files[level].length;) {
      final FileMetaData fileMetaData = files[level][i++];
      final ByteBuffer fileStart = fileMetaData.getSmallest().getUserKey();
      final ByteBuffer fileLimit = fileMetaData.getLargest().getUserKey();
      if (userComparator.compare(fileLimit, userBegin) >= 0
          && userComparator.compare(fileStart, userEnd) <= 0) {
        inputs.add(fileMetaData);
        if (level == 0) {
          if (userComparator.compare(fileStart, userBegin) < 0) {
            userBegin = fileStart;
            inputs.clear();
            i = 0;
          } else if (userComparator.compare(fileLimit, userEnd) > 0) {
            userEnd = fileLimit;
            inputs.clear();
            i = 0;
          }
        }
      }
    }
    return inputs;
  }

  public int numberOfFilesInLevel(final int level) {
    return files[level].length;
  }

  public FileMetaData[] getFiles(final int level) {
    return files[level];
  }

  public FileMetaData[][] getFiles() {
    return files;
  }

  private boolean updateStats(final int seekFileLevel, final FileMetaData seekFile) {
    if (seekFile == null) {
      return false;
    }

    seekFile.decrementAllowedSeeks();
    if (seekFile.getAllowedSeeks() <= 0) {
      Entry<FileMetaData, Integer> current;
      do {
        current = seekCompaction.get();
        if (current != null) {
          return false;
        }
      } while (!seekCompaction.compareAndSet(current,
          Maps.immutableEntry(seekFile, seekFileLevel)));
      return true;
    }
    return false;
  }

  public Entry<FileMetaData, Integer> getSeekCompaction() {
    return seekCompaction.get();
  }

  public CompletionStage<Long> getApproximateOffsetOf(final InternalKey key) {
    long result = 0;
    final Stream.Builder<CompletionStage<Long>> tableReads = Stream.builder();
    for (int level = 0; level < files.length; level++) {
      for (final FileMetaData fileMetaData : files[level]) {
        if (internalKeyComparator.compare(fileMetaData.getLargest(), key) <= 0) {
          // Entire file is before "key", so just add the file size
          result += fileMetaData.getFileSize();
        } else if (internalKeyComparator.compare(fileMetaData.getSmallest(), key) > 0) {
          // Entire file is after "key", so ignore
          if (level > 0) {
            // Files other than level 0 are sorted by meta.smallest, so
            // no further files in this level will contain data for
            // "Key".
            break;
          }
        } else {
          // "key" falls in the range for this table. Add the
          // approximate offset of "key" within the table.
          tableReads.add(tableCache.getApproximateOffsetOf(fileMetaData, key));
        }
      }
    }
    final long prelimResult = result;
    return CompletableFutures.allOf(tableReads.build())
        .thenApply(longStream -> longStream.mapToLong(Long::longValue).sum() + prelimResult);
  }

  static final class LevelIterator extends
      TwoStageIterator<ReverseSeekingIterator<InternalKey, FileMetaData>, TableIterator, FileMetaData> {
    private final TableCache tableCache;

    public LevelIterator(final TableCache tableCache, final FileMetaData[] files,
        final Comparator<InternalKey> comparator) {
      super(new FileIterator(files, comparator));
      this.tableCache = tableCache;
    }

    @Override
    protected CompletionStage<TableIterator> getData(final FileMetaData file) {
      return tableCache.tableIterator(file);
    }

    private static final class FileIterator
        implements ReverseSeekingIterator<InternalKey, FileMetaData> {
      private final FileMetaData[] files;
      private final Comparator<InternalKey> comparator;
      private int index;

      public FileIterator(final FileMetaData[] files, final Comparator<InternalKey> comparator) {
        this.files = files;
        this.comparator = comparator;
      }

      @Override
      public void seekToFirst() {
        index = 0;
      }

      @Override
      public void seek(final InternalKey targetKey) {
        if (files.length == 0) {
          return;
        }

        // seek the index to the block containing the key
        index = findFile(files, targetKey, comparator);

        // if the index is now pointing to the last block in the file, check if the largest key in
        // the block is less than the target key. If so, we need to seek beyond the end of this file
        if (index == files.length - 1
            && comparator.compare(files[index].getLargest(), targetKey) < 0) {
          index++;
        }
      }

      @Override
      public Entry<InternalKey, FileMetaData> peek() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Entry<InternalKey, FileMetaData> next() {
        final FileMetaData f = files[index++];
        return Maps.immutableEntry(f.getLargest(), f);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean hasNext() {
        return index < files.length;
      }

      @Override
      public Entry<InternalKey, FileMetaData> peekPrev() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Entry<InternalKey, FileMetaData> prev() {
        final FileMetaData f = files[--index];
        return Maps.immutableEntry(f.getLargest(), f);
      }

      @Override
      public boolean hasPrev() {
        return index > 0;
      }

      @Override
      public void seekToEnd() {
        index = files.length;
      }
    }
  }
}
