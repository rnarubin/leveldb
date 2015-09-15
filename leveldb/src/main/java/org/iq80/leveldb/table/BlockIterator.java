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

package org.iq80.leveldb.table;

import static org.iq80.leveldb.util.SizeOf.SIZE_OF_INT;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.NoSuchElementException;
import java.util.function.Function;

import org.iq80.leveldb.impl.ReverseSeekingIterator;
import org.iq80.leveldb.util.ByteBuffers;
import org.iq80.leveldb.util.VariableLengthQuantity;

import com.google.common.base.Preconditions;

public final class BlockIterator<T> implements ReverseSeekingIterator<T, ByteBuffer> {
  private final ByteBuffer data;
  private final ByteBuffer restartPositions;
  private final int restartCount;
  private final Comparator<T> comparator;
  private final Function<ByteBuffer, T> decoder;

  private final Deque<CacheEntry<T>> prevCache;
  private int prevCacheRestartIndex;

  private int prevPosition;
  private int restartIndex;
  private BlockEntry<T> nextEntry;
  private BlockEntry<T> prevEntry;


  public BlockIterator(final ByteBuffer data, final ByteBuffer restartPositions,
      final Comparator<T> comparator, final Function<ByteBuffer, T> decoder) {
    Preconditions.checkArgument(restartPositions.remaining() % SIZE_OF_INT == 0,
        "restartPositions.readableBytes() must be a multiple of %s", SIZE_OF_INT);

    this.decoder = decoder;
    this.data = ByteBuffers.duplicate(data);

    this.restartPositions = ByteBuffers.slice(restartPositions);
    this.restartCount = this.restartPositions.remaining() / SIZE_OF_INT;

    this.comparator = comparator;

    prevCache = new ArrayDeque<CacheEntry<T>>();
    prevCacheRestartIndex = -1;

    seekToFirst();
  }

  @Override
  public boolean hasNext() {
    return nextEntry != null;
  }

  @Override
  public boolean hasPrev() {
    return prevEntry != null || currentPosition() > 0;
  }

  @Override
  public BlockEntry<T> peek() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return nextEntry;
  }

  @Override
  public BlockEntry<T> peekPrev() {
    if (prevEntry == null && currentPosition() > 0) {
      // this case should only occur after seeking under certain conditions
      final BlockEntry<T> peeked = prev();
      next();
      prevEntry = peeked;
    } else if (prevEntry == null) {
      throw new NoSuchElementException();
    }
    return prevEntry;
  }

  @Override
  public BlockEntry<T> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    resetCache();
    prevEntry = nextEntry;

    if (!data.hasRemaining()) {
      nextEntry = null;
    } else {
      // read entry at current data position
      nextEntry = readEntry(data, prevEntry);
    }
    return prevEntry;
  }

  @Override
  public BlockEntry<T> prev() {
    final int original = currentPosition();
    if (original == 0) {
      throw new NoSuchElementException();
    }

    final int previousRestart = getPreviousRestart(restartIndex, original);
    if (previousRestart == prevCacheRestartIndex && prevCache.size() > 0) {
      final CacheEntry<T> prevState = prevCache.pop();
      nextEntry = prevState.entry;
      prevPosition = prevState.prevPosition;
      data.position(prevState.dataPosition);

      final CacheEntry<T> peek = prevCache.peek();
      prevEntry = peek == null ? null : peek.entry;
    } else {
      seekToRestartPosition(previousRestart);
      prevCacheRestartIndex = previousRestart;
      prevCache.push(new CacheEntry<T>(nextEntry, prevPosition, data.position()));
      while (data.position() < original && data.hasRemaining()) {
        prevEntry = nextEntry;
        nextEntry = readEntry(data, prevEntry);
        prevCache.push(new CacheEntry<T>(nextEntry, prevPosition, data.position()));
      }
      prevCache.pop(); // we don't want to cache the last entry because that's returned with this
                       // call
    }

    return nextEntry;
  }

  private int getPreviousRestart(int startIndex, final int position) {
    while (getRestartPoint(startIndex) >= position) {
      if (startIndex == 0) {
        throw new NoSuchElementException();
      }
      startIndex--;
    }
    return startIndex;
  }

  private int currentPosition() {
    // lags data.position because of the nextEntry read-ahead
    if (nextEntry != null) {
      return prevPosition;
    }
    return data.position();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  /**
   * Repositions the iterator to the beginning of this block.
   */
  @Override
  public void seekToFirst() {
    if (restartCount > 0) {
      seekToRestartPosition(0);
    }
  }

  void seekToLast() {
    if (restartCount > 0) {
      seekToRestartPosition(Math.max(0, restartCount - 1));
      while (data.hasRemaining()) {
        // seek until reaching the last entry
        next();
      }
    }
  }

  @Override
  public void seekToEnd() {
    if (restartCount > 0) {
      // TODO might be able to accomplish with setting data position
      seekToLast();
      next();
    }
  }

  /**
   * Repositions the iterator so the key of the next BlockElement returned greater than or equal to
   * the specified targetKey.
   */
  @Override
  public void seek(final T targetKey) {
    if (restartCount == 0) {
      return;
    }

    int left = 0;
    int right = restartCount - 1;

    // binary search restart positions to find the restart position immediately before the targetKey
    while (left < right) {
      final int mid = (left + right + 1) / 2;

      seekToRestartPosition(mid);

      if (comparator.compare(nextEntry.getKey(), targetKey) < 0) {
        // key at mid is smaller than targetKey. Therefore all restart
        // blocks before mid are uninteresting.
        left = mid;
      } else {
        // key at mid is greater than or equal to targetKey. Therefore
        // all restart blocks at or after mid are uninteresting.
        right = mid - 1;
      }
    }

    // linear search (within restart block) for first key greater than or equal to targetKey
    for (seekToRestartPosition(left); nextEntry != null; next()) {
      if (comparator.compare(peek().getKey(), targetKey) >= 0) {
        break;
      }
    }
  }

  /**
   * Seeks to and reads the entry at the specified restart position.
   * <p/>
   * After this method, nextEntry will contain the next entry to return, and the previousEntry will
   * be null.
   */
  private void seekToRestartPosition(final int restartPosition) {
    Preconditions.checkPositionIndex(restartPosition, restartCount, "restartPosition");

    // seek data readIndex to the beginning of the restart block
    data.position(getRestartPoint(restartPosition));

    resetCache();
    // clear the entries to assure key is not prefixed
    nextEntry = null;
    prevEntry = null;

    restartIndex = restartPosition;

    // read the entry
    nextEntry = readEntry(data, null);
  }

  private int getRestartPoint(final int index) {
    return restartPositions.getInt(index * SIZE_OF_INT);
  }

  /**
   * Reads the entry at the current data readIndex. After this method, data readIndex is positioned
   * at the beginning of the next entry or at the end of data if there was not a next entry.
   *
   * @return true if an entry was read
   */
  private BlockEntry<T> readEntry(final ByteBuffer data, final BlockEntry<T> previousEntry) {
    prevPosition = data.position();

    // read entry header
    final int sharedKeyLength = VariableLengthQuantity.readVariableLengthInt(data);
    final int nonSharedKeyLength = VariableLengthQuantity.readVariableLengthInt(data);
    final int valueLength = VariableLengthQuantity.readVariableLengthInt(data);

    // read key
    final ByteBuffer key;
    if (sharedKeyLength > 0) {
      Preconditions.checkState(previousEntry != null,
          "Entry has a shared key but no previous entry was provided");
      final ByteBuffer prev = previousEntry.getEncodedKey();
      key =
          ByteBuffer.allocate(sharedKeyLength + nonSharedKeyLength).order(ByteOrder.LITTLE_ENDIAN);
      key.put(ByteBuffers.duplicateByLength(prev, prev.position(), sharedKeyLength));
      ByteBuffers.putLength(key, data, nonSharedKeyLength);
      key.flip();
    } else {
      key = ByteBuffers.duplicateAndAdvance(data, nonSharedKeyLength);
    }

    // read value
    final ByteBuffer value = ByteBuffers.duplicateAndAdvance(data, valueLength);

    return BlockEntry.of(decoder.apply(key), key, value);
  }

  private void resetCache() {
    prevCache.clear();
    prevCacheRestartIndex = -1;
  }

  private static class CacheEntry<T> {
    public final BlockEntry<T> entry;
    public final int prevPosition, dataPosition;

    public CacheEntry(final BlockEntry<T> entry, final int prevPosition, final int dataPosition) {
      this.entry = entry;
      this.prevPosition = prevPosition;
      this.dataPosition = dataPosition;
    }
  }
}
