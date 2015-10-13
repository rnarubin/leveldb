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

import static org.iq80.leveldb.util.Iterators.Direction.FORWARD;
import static org.iq80.leveldb.util.Iterators.Direction.REVERSE;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import org.iq80.leveldb.SeekingAsynchronousIterator;
import org.iq80.leveldb.util.Iterators.Direction;

import com.google.common.collect.Maps;

public final class SnapshotSeekingIterator
    implements SeekingAsynchronousIterator<ByteBuffer, ByteBuffer> {
  private final SeekingAsynchronousIterator<InternalKey, ByteBuffer> iterator;
  private final long sequence;
  private final Comparator<ByteBuffer> userComparator;
  private final Executor asyncExec;
  private Direction direction;
  private Entry<InternalKey, ByteBuffer> savedEntry;
  private Optional<Entry<InternalKey, ByteBuffer>> savedPrev;

  public SnapshotSeekingIterator(
      final SeekingAsynchronousIterator<InternalKey, ByteBuffer> iterator,
      final SnapshotImpl snapshot, final Comparator<ByteBuffer> userComparator,
      final Executor asyncExec) {
    this.iterator = iterator;
    this.sequence = snapshot.getLastSequence();
    this.userComparator = userComparator;
    this.asyncExec = asyncExec;
  }

  private static Entry<ByteBuffer, ByteBuffer> externalize(
      final Entry<InternalKey, ByteBuffer> internalEntry) {
    return Maps.immutableEntry(internalEntry.getKey().getUserKey(), internalEntry.getValue());
  }

  @Override
  public CompletionStage<Void> asyncClose() {
    return iterator.asyncClose();
  }

  @Override
  public CompletionStage<Void> seekToFirst() {
    direction = FORWARD;
    savedEntry = null;
    savedPrev = null;
    return iterator.seekToFirst();
  }

  @Override
  public CompletionStage<Void> seekToEnd() {
    direction = REVERSE;
    savedEntry = null;
    savedPrev = null;
    return iterator.seekToEnd();
  }

  @Override
  public CompletionStage<Void> seek(final ByteBuffer key) {
    direction = null;
    savedEntry = null;
    savedPrev = null;
    return iterator.seek(new TransientInternalKey(key, sequence, ValueType.VALUE));
  }

  @Override
  public CompletionStage<Optional<Entry<ByteBuffer, ByteBuffer>>> next() {
    if (direction == REVERSE) {
      // direction = FORWARD;
      //// advance twice because prevUserEntry is consuming
      // return (savedPrev == null ? iterator.next()
      // : iterator.next().thenCompose(ignored -> iterator.next())).thenApply(optNext -> {
      // savedEntry = optNext.orElse(null);
      // if (savedEntry == null) {
      // direction = REVERSE;
      // }
      // return optNext.map(SnapshotSeekingIterator::externalize);
      // });
    } else {
      return nextUserEntry(savedEntry == null ? null : savedEntry.getKey().getUserKey());
    }
  }

  @Override
  public CompletionStage<Optional<Entry<ByteBuffer, ByteBuffer>>> prev() {
    if (direction == FORWARD) {
      // direction = REVERSE;
      //// the last valid entry was returned by next
      //// so iterator's prev must be the valid entry
      // return iterator.prev().thenApply(optPrev -> {
      // savedEntry = optPrev.orElse(null);
      // if (savedEntry == null) {
      // direction = FORWARD;
      // }
      // return optPrev.map(SnapshotSeekingIterator::externalize);
      // });
    } else {
      return prevUserEntry(savedPrev).thenApply(optSave -> {
        savedPrev = optSave;
        return Optional.ofNullable(savedEntry).map(SnapshotSeekingIterator::externalize);
      });
    }
  }

  // TODO(optimization) don't asyncExec until certain stack depth
  private CompletionStage<Optional<Entry<ByteBuffer, ByteBuffer>>> nextUserEntry(
      final ByteBuffer skipKey) {
    return iterator.next().thenComposeAsync(optNext -> {
      if (optNext.isPresent()) {
        final InternalKey internalKey = optNext.get().getKey();

        if (internalKey.getSequenceNumber() <= sequence) {
          switch (internalKey.getValueType()) {
            case DELETION:
              return nextUserEntry(internalKey.getUserKey());
            case VALUE:
              if (skipKey == null
                  || userComparator.compare(internalKey.getUserKey(), skipKey) > 0) {
                // no longer skipping, or found the next larger key
                savedEntry = optNext.get();
                return CompletableFuture.completedFuture(Optional.of(externalize(savedEntry)));
              }
              return nextUserEntry(skipKey);
            default:
              throw new IllegalStateException("unknown value type:" + internalKey.getValueType());
          }
        } else {
          // newer than snapshot, keep going
          return nextUserEntry(skipKey);
        }
      } else {
        // out of entries
        direction = REVERSE;
        savedEntry = null;
        return CompletableFuture.completedFuture(Optional.empty());
      }
    } , asyncExec);
  }

  // private CompletionStage<Optional<Entry<InternalKey, ByteBuffer>>> prevUserEntry(
  // final Optional<Entry<InternalKey, ByteBuffer>> saved) {
  // return prevStep(saved != null ? CompletableFuture.completedFuture(saved) : iterator.prev(),
  // ValueType.DELETION);
  // }

  private CompletionStage<Optional<Entry<InternalKey, ByteBuffer>>> prevUserEntry(
      final CompletionStage<Optional<Entry<InternalKey, ByteBuffer>>> prev,
      final ValueType valueType) {
    return prev.thenComposeAsync(optPrev -> {
      if (optPrev.isPresent()) {
        final InternalKey internalKey = optPrev.get().getKey();
        if (internalKey.getSequenceNumber() <= sequence) {
          if (valueType == ValueType.VALUE && (savedEntry == null || userComparator
              .compare(internalKey.getUserKey(), savedEntry.getKey().getUserKey()) < 0)) {
            return CompletableFuture.completedFuture(optPrev);
          }
          final ValueType newType = internalKey.getValueType();
          if (newType == ValueType.DELETION) {
            savedEntry = null;
          } else {
            savedEntry = optPrev.get();
          }
          return prevUserEntry(iterator.prev(), newType);

        } else if (valueType == ValueType.VALUE) {
          return CompletableFuture.completedFuture(optPrev);
        } else {
          return prevUserEntry(iterator.prev(), valueType);
        }
      } else {
        if (valueType == ValueType.DELETION) {
          savedEntry = null;
          direction = FORWARD;
        }
        return CompletableFuture.completedFuture(Optional.empty());
      }
    } , asyncExec);
  }

  @Override
  public String toString() {
    return "SnapshotSeekingIterator [sequence=" + sequence + ", iterator=" + iterator + "]";
  }
}
