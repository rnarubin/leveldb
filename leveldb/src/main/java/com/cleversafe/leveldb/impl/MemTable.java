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

import static com.cleversafe.leveldb.util.Iterators.Direction.FORWARD;
import static com.cleversafe.leveldb.util.Iterators.Direction.REVERSE;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import com.cleversafe.leveldb.util.Iterators.Direction;
import com.cleversafe.leveldb.util.SeekingAsynchronousIterator;

public class MemTable {
  private final NavigableMap<InternalKey, ByteBuffer> table;
  private final AtomicLong approximateMemoryUsage = new AtomicLong();
  private final Comparator<ByteBuffer> userComparator;

  public MemTable(final InternalKeyComparator internalKeyComparator) {
    this.table = new ConcurrentSkipListMap<InternalKey, ByteBuffer>(internalKeyComparator);
    this.userComparator = internalKeyComparator.getUserComparator();
  }

  public void clear() {
    table.clear();
  }

  public boolean isEmpty() {
    return table.isEmpty();
  }

  public long approximateMemoryUsage() {
    return approximateMemoryUsage.get();
  }

  protected long getAndAddApproximateMemoryUsage(final long delta) {
    return approximateMemoryUsage.getAndAdd(delta);
  }

  public void add(final InternalKey internalKey, final ByteBuffer value) {
    table.put(internalKey, value);
  }

  public LookupResult get(final LookupKey key) {
    assert key != null : "key is null";

    final InternalKey internalKey = key.getInternalKey();
    final Entry<InternalKey, ByteBuffer> entry = table.ceilingEntry(internalKey);
    if (entry == null) {
      return null;
    }

    final InternalKey entryKey = entry.getKey();
    if (userComparator.compare(entryKey.getUserKey(), internalKey.getUserKey()) == 0) {
      if (entryKey.getValueType() == ValueType.DELETION) {
        return LookupResult.deleted(key);
      } else {
        return LookupResult.ok(key, entry.getValue());
      }
    }
    return null;
  }

  public Iterable<Entry<InternalKey, ByteBuffer>> entryIterable() {
    return table.entrySet();
  }

  public SeekingAsynchronousIterator<InternalKey, ByteBuffer> iterator() {
    return new MemTableIterator();
  }

  class MemTableIterator implements SeekingAsynchronousIterator<InternalKey, ByteBuffer> {
    private Iterator<Entry<InternalKey, ByteBuffer>> iter;
    private InternalKey lastKey;
    private Direction direction;

    public MemTableIterator() {}

    @Override
    public CompletionStage<Optional<Entry<InternalKey, ByteBuffer>>> next() {
      if (direction != FORWARD) {
        // direction is reverse or null
        if (lastKey == null) {
          // must be from seekToEnd
          assert direction == REVERSE;
          return CompletableFuture.completedFuture(Optional.empty());
        }
        iter = table.tailMap(lastKey, true).entrySet().iterator();
        direction = FORWARD;
      }

      if (iter.hasNext()) {
        final Entry<InternalKey, ByteBuffer> next = iter.next();
        lastKey = next.getKey();
        return CompletableFuture.completedFuture(Optional.of(next));
      } else {
        return CompletableFuture.completedFuture(Optional.empty());
      }
    }

    @Override
    public CompletionStage<Optional<Entry<InternalKey, ByteBuffer>>> prev() {
      if (direction != REVERSE) {
        if (lastKey == null) {
          assert direction == FORWARD;
          return CompletableFuture.completedFuture(Optional.empty());
        }
        iter = table.descendingMap().tailMap(lastKey, direction == FORWARD).entrySet().iterator();
        direction = REVERSE;
      }

      if (iter.hasNext()) {
        final Entry<InternalKey, ByteBuffer> next = iter.next();
        lastKey = next.getKey();
        return CompletableFuture.completedFuture(Optional.of(next));
      } else {
        return CompletableFuture.completedFuture(Optional.empty());
      }
    }

    @Override
    public CompletionStage<Void> seekToFirst() {
      lastKey = null;
      direction = FORWARD;
      iter = table.entrySet().iterator();
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> seek(final InternalKey key) {
      lastKey = key;
      direction = null;
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> seekToEnd() {
      lastKey = null;
      direction = REVERSE;
      iter = table.descendingMap().entrySet().iterator();
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> asyncClose() {
      return CompletableFuture.completedFuture(null);
    }
  }
}
