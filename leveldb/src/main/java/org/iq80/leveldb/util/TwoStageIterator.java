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

import static org.iq80.leveldb.util.Iterators.Direction.NEXT;
import static org.iq80.leveldb.util.Iterators.Direction.PREV;

import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.iq80.leveldb.SeekingAsynchronousIterator;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.ReverseSeekingIterator;
import org.iq80.leveldb.util.Iterators.Direction;

public abstract class TwoStageIterator<IndexT extends ReverseSeekingIterator<InternalKey, V>, DataT extends SeekingAsynchronousIterator<InternalKey, ByteBuffer>, V>
    implements SeekingAsynchronousIterator<InternalKey, ByteBuffer> {
  private final IndexT index;
  private DataT current;
  private Direction currentOrigin;

  public TwoStageIterator(final IndexT indexIterator) {
    this.index = indexIterator;
  }

  @Override
  public CompletionStage<Void> seekToFirst() {
    final CompletionStage<Void> close = Closeables.asyncClose(current);
    index.seekToFirst();
    current = null;
    return close;
  }

  @Override
  public CompletionStage<Void> seekToEnd() {
    final CompletionStage<Void> close = Closeables.asyncClose(current);
    index.seekToEnd();
    current = null;
    return close;
  }

  @Override
  public CompletionStage<Void> seek(final InternalKey targetKey) {
    final CompletionStage<Void> close = Closeables.asyncClose(current);

    index.seek(targetKey);
    if (index.hasNext()) {
      // seek the current iterator to the key
      return getData(index.next().getValue()).thenCompose(newCurrent -> {
        current = newCurrent;
        currentOrigin = NEXT;
        return newCurrent.seek(targetKey);
      }).thenCombine(close, (seeked, closed) -> null);
    } else {
      current = null;
      return close;
    }
  }

  @Override
  public CompletionStage<Void> asyncClose() {
    return Closeables.asyncClose(current);
  }

  @Override
  public CompletionStage<Optional<Entry<InternalKey, ByteBuffer>>> next() {
    return current == null ? advanceIndex(NEXT) : advanceData(NEXT);
  }

  @Override
  public CompletionStage<Optional<Entry<InternalKey, ByteBuffer>>> prev() {
    return current == null ? advanceIndex(PREV) : advanceData(PREV);
  }

  private CompletionStage<Optional<Entry<InternalKey, ByteBuffer>>> advanceData(
      final Direction direction) {
    assert current != null;
    final CompletionStage<Optional<Entry<InternalKey, ByteBuffer>>> init =
        direction.asyncAdvance(current);
    return init.<Optional<Entry<InternalKey, ByteBuffer>>>thenCompose(entry -> {
      if (entry.isPresent()) {
        return init;
      }

      final CompletionStage<Void> close = current.asyncClose();
      if (currentOrigin == opposite(direction)) {
        if (!direction.hasMore(index)) {
          return close.thenApply(voided -> Optional.empty());
        }
        direction.advance(index);
      }
      return close.thenCombine(advanceIndex(direction), (closed, advanced) -> advanced);
    });
  }

  private CompletionStage<Optional<Entry<InternalKey, ByteBuffer>>> advanceIndex(
      final Direction direction) {
    return direction.hasMore(index)
        ? getData(direction.advance(index).getValue()).thenCompose(newCurrent -> {
          current = newCurrent;
          currentOrigin = direction;
          return direction.seekToEdge(current).thenCompose(voided -> advanceData(direction));
        }) : CompletableFuture.completedFuture(Optional.empty());
  }

  private static Direction opposite(final Direction direction) {
    switch (direction) {
      case NEXT:
        return PREV;
      case PREV:
        return NEXT;
      default:
        throw new IllegalArgumentException("Not a valid direction:" + direction);
    }
  }

  protected abstract CompletionStage<DataT> getData(V indexValue);

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("TwoStageIterator");
    sb.append("{index=").append(index);
    sb.append(", current=").append(current);
    sb.append('}');
    return sb.toString();
  }
}
