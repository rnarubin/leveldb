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
package org.iq80.leveldb.util;

import java.util.Map.Entry;
import java.util.concurrent.CompletionStage;

import org.iq80.leveldb.impl.FileMetaData;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.InternalKeyComparator;
import org.iq80.leveldb.impl.ReverseSeekingIterator;
import org.iq80.leveldb.impl.TableCache;
import org.iq80.leveldb.table.TableIterator;

import com.google.common.collect.Maps;

public final class LevelIterator extends
    TwoStageIterator<ReverseSeekingIterator<InternalKey, FileMetaData>, TableIterator, FileMetaData> {
  private final TableCache tableCache;

  public LevelIterator(final TableCache tableCache, final FileMetaData[] files,
      final InternalKeyComparator comparator) {
    super(new FileIterator(files, comparator));
    this.tableCache = tableCache;
  }

  @Override
  protected CompletionStage<TableIterator> getData(final FileMetaData file) {
    return tableCache.newIterator(file);
  }

  private static final class FileIterator
      implements ReverseSeekingIterator<InternalKey, FileMetaData> {
    private final FileMetaData[] files;
    private final InternalKeyComparator comparator;
    private int index;

    public FileIterator(final FileMetaData[] files, final InternalKeyComparator comparator) {
      this.files = files;
      this.comparator = comparator;
    }

    @Override
    public void seekToFirst() {
      index = 0;
    }

    @Override
    public void seek(final InternalKey targetKey) {
      // seek the index to the block containing the key
      if (files.length == 0) {
        return;
      }

      // todo replace with Collections.binarySearch
      int left = 0;
      int right = files.length - 1;

      // binary search restart positions to find the restart position immediately before the
      // targetKey
      while (left < right) {
        final int mid = (left + right) / 2;

        if (comparator.compare(files[mid].getLargest(), targetKey) < 0) {
          // Key at "mid.largest" is < "target". Therefore all
          // files at or before "mid" are uninteresting.
          left = mid + 1;
        } else {
          // Key at "mid.largest" is >= "target". Therefore all files
          // after "mid" are uninteresting.
          right = mid;
        }
      }
      index = right;

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
