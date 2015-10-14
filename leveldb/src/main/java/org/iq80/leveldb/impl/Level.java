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

import static org.iq80.leveldb.impl.SequenceNumber.MAX_SEQUENCE_NUMBER;
import static org.iq80.leveldb.impl.ValueType.VALUE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.iq80.leveldb.MemoryManager;
import org.iq80.leveldb.SeekingAsynchronousIterator;
import org.iq80.leveldb.util.ByteBuffers;
import org.iq80.leveldb.util.CompletableFutures;
import org.iq80.leveldb.util.InternalIterator;
import org.iq80.leveldb.util.LevelIterator;
import org.iq80.leveldb.util.MergingIterator;

import com.google.common.base.Preconditions;

public final class Level {
  private final int levelNumber;
  private final TableCache tableCache;
  private final InternalKeyComparator internalKeyComparator;
  private final FileMetaData[] files;

  public Level(final int levelNumber, final FileMetaData[] files, final TableCache tableCache,
      final InternalKeyComparator internalKeyComparator) {
    this.files = files;
    this.tableCache = tableCache;
    this.internalKeyComparator = internalKeyComparator;
    this.levelNumber = levelNumber;
  }

  public int getLevelNumber() {
    return levelNumber;
  }

  public FileMetaData[] getFiles() {
    return files;
  }

  public CompletionStage<SeekingAsynchronousIterator<InternalKey, ByteBuffer>> getIterator() {
    if (levelNumber == 0) {
      return CompletableFutures.allOf(Stream.of(files).map(tableCache::newIterator))
          .thenApply(iterStream -> MergingIterator
              .newMergingIterator(iterStream.collect(Collectors.toList()), internalKeyComparator));
    } else {
      return CompletableFuture
          .completedFuture(new LevelIterator(tableCache, files, internalKeyComparator));
    }
  }

  // TODO(optimization) collapse into version, save some completion chaining
  public CompletionStage<LookupResult> get(final LookupKey key, final ReadStats readStats,
      final MemoryManager memory) throws IOException {
    if (files.length == 0) {
      return CompletableFuture.completedFuture(null);
    }

    final List<FileMetaData> fileMetaDataList;
    if (levelNumber == 0) {
      fileMetaDataList = new ArrayList<>();
      for (final FileMetaData fileMetaData : files) {
        if (internalKeyComparator.getUserComparator().compare(key.getUserKey(),
            fileMetaData.getSmallest().getUserKey()) >= 0
            && internalKeyComparator.getUserComparator().compare(key.getUserKey(),
                fileMetaData.getLargest().getUserKey()) <= 0) {
          fileMetaDataList.add(fileMetaData);
        }
      }

      if (fileMetaDataList.isEmpty()) {
        return CompletableFuture.completedFuture(null);
      }

      // sort descending by file number
      Collections.sort(fileMetaDataList, (o1, o2) -> Long.compare(o2.getNumber(), o1.getNumber()));
    } else {
      // Binary search to find earliest index whose largest key >= ikey.
      final int index = findFile(key.getInternalKey());

      // did we find any files that could contain the key?
      if (index >= files.length) {
        return CompletableFuture.completedFuture(null);
      }

      // check if the smallest user key in the file is less than the target user key
      final FileMetaData fileMetaData = files[index];
      if (internalKeyComparator.getUserComparator().compare(key.getUserKey(),
          fileMetaData.getSmallest().getUserKey()) < 0) {
        return CompletableFuture.completedFuture(null);
      }

      // search this file
      fileMetaDataList = Collections.singletonList(fileMetaData);
    }

    readStats.clear();

    assert !fileMetaDataList.isEmpty();
    final Iterator<FileMetaData> fileIter = fileMetaDataList.iterator();
    lookup(fileIter.next(), null, readStats);

    for (final FileMetaData fileMetaData : fileMetaDataList) {
      // open the iterator
      try (InternalIterator iterator = tableCache.newIterator(fileMetaData)) {
        // seek to the key
        iterator.seek(key.getInternalKey());

        if (iterator.hasNext()) {
          // parse the key in the block
          final Entry<InternalKey, ByteBuffer> entry = iterator.next();
          final InternalKey internalKey = entry.getKey();
          Preconditions.checkState(internalKey != null, "Corrupt key for %s",
              key.getUserKey().toString());

          // if this is a value key (not a delete) and the keys match, return the value
          if (ByteBuffers.compare(key.getUserKey(), internalKey.getUserKey()) == 0) {
            if (internalKey.getValueType() == ValueType.DELETION) {
              return LookupResult.deleted(key);
            } else if (internalKey.getValueType() == VALUE) {
              return LookupResult.ok(key, ByteBuffers.copy(entry.getValue(), memory), true);
            }
          }
        }
      }
    }

    return CompletableFuture.completedFuture(null);
  }

  private CompletionStage<LookupResult> lookup(final FileMetaData file, final FileMetaData prevFile,
      final ReadStats readStats) {
    tableCache.newIterator(file).thenCompose(iter -> {
      if (prevFile != null && readStats.getSeekFile() == null) {
        readStats.setSeekFile(file);
        readStats.setSeekFileLevel(levelNumber);
      }
    });

    return null;
  }

  public boolean someFileOverlapsRange(final ByteBuffer smallestUserKey,
      final ByteBuffer largestUserKey) {
    final InternalKey smallestInternalKey =
        new TransientInternalKey(smallestUserKey, MAX_SEQUENCE_NUMBER, VALUE);
    final int index = findFile(smallestInternalKey);

    return ((index < files.length) && internalKeyComparator.getUserComparator()
        .compare(largestUserKey, files[index].getSmallest().getUserKey()) >= 0);
  }

  private int findFile(final InternalKey targetKey) {
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

  @Override
  public String toString() {
    return "Level [levelNumber=" + levelNumber + ", files=" + Arrays.toString(files) + "]";
  }
}
