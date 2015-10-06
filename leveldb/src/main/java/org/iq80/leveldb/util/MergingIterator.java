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

import static org.iq80.leveldb.util.Iterators.Direction.FORWARD;
import static org.iq80.leveldb.util.Iterators.Direction.REVERSE;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.iq80.leveldb.SeekingAsynchronousIterator;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.util.Iterators.Direction;

public final class MergingIterator implements SeekingAsynchronousIterator<InternalKey, ByteBuffer> {
  /*
   * favors forward iteration by pre-loading next() after seek(key)
   */

  private final OrdinalIterator[] iters;
  private final Comparator<OrdinalIterator> smallerNext, largerPrev;
  private boolean uncleanReverse;

  private MergingIterator(
      final Collection<SeekingAsynchronousIterator<InternalKey, ByteBuffer>> iterators,
      final Comparator<InternalKey> internalKeyComparator) {
    assert iterators.size() > 1;
    int ordinal = 0;
    this.iters = new OrdinalIterator[iterators.size()];
    for (final SeekingAsynchronousIterator<InternalKey, ByteBuffer> iter : iterators) {
      iters[ordinal] = new OrdinalIterator(ordinal, iter);
      ordinal++;
    }

    this.smallerNext = OrdinalIterator.smallerNext(internalKeyComparator);
    this.largerPrev = OrdinalIterator.largerPrev(internalKeyComparator);
  }

  public static SeekingAsynchronousIterator<InternalKey, ByteBuffer> newMergingIterator(
      final List<SeekingAsynchronousIterator<InternalKey, ByteBuffer>> iterators,
      final Comparator<InternalKey> internalKeyComparator) {
    switch (iterators.size()) {
      case 0:
        return Iterators.emptySeekingAsyncIterator();
      case 1:
        return iterators.get(0);
      default:
        return new MergingIterator(iterators, internalKeyComparator);
    }
  }

  @Override
  public CompletionStage<Optional<Entry<InternalKey, ByteBuffer>>> next() {
    return null;
  }

  @Override
  public CompletionStage<Optional<Entry<InternalKey, ByteBuffer>>> prev() {
    if (uncleanReverse) {
      // do work
      uncleanReverse = false;
    }
    return null;
  }

  @Override
  public CompletionStage<Void> asyncClose() {
    return CompletableFutures.allOfVoid(Stream.of(iters).map(OrdinalIterator::asyncClose));
  }

  @Override
  public CompletionStage<Void> seekToFirst() {
    return CompletableFutures.allOfVoid(Stream.of(iters).map(OrdinalIterator::seekToFirst));
  }

  @Override
  public CompletionStage<Void> seekToEnd() {
    return CompletableFutures.allOfVoid(Stream.of(iters).map(OrdinalIterator::seekToEnd));
  }

  @Override
  public CompletionStage<Void> seek(final InternalKey key) {
    uncleanReverse = true;
    return CompletableFutures.allOfVoid(Stream.of(iters).map(ord -> ord.seek(key)));
  }

  private static final class OrdinalIterator {
    private final SeekingAsynchronousIterator<InternalKey, ByteBuffer> iterator;
    private final int ordinal;
    private Optional<Entry<InternalKey, ByteBuffer>> cachedNext, cachedPrev;
    private Direction lastAdvance;

    public OrdinalIterator(final int ordinal,
        final SeekingAsynchronousIterator<InternalKey, ByteBuffer> iterator) {
      this.ordinal = ordinal;
      this.iterator = iterator;
    }

    public CompletionStage<Void> seekToFirst() {
      cachedPrev = Optional.empty();
      lastAdvance = FORWARD;
      return iterator.seekToFirst().thenCompose(voided -> iterator.next())
          .thenAccept(optNext -> cachedNext = optNext);
    }

    public CompletionStage<Void> seekToEnd() {
      cachedNext = Optional.empty();
      lastAdvance = REVERSE;
      return iterator.seekToEnd().thenCompose(voided -> iterator.prev())
          .thenAccept(optPrev -> cachedPrev = optPrev);
    }

    public CompletionStage<Void> seek(final InternalKey key) {
      cachedPrev = null;
      lastAdvance = FORWARD;
      return iterator.seek(key).thenCompose(voided -> iterator.next())
          .thenAccept(optNext -> cachedNext = optNext);
    }

    public CompletionStage<Void> asyncClose() {
      return iterator.asyncClose();
    }

    public CompletionStage<Optional<Entry<InternalKey, ByteBuffer>>> pollNext() {
      assert lastAdvance != null;
      assert cachedNext != null;
      if (lastAdvance == FORWARD) {
        return iterator.next().thenApply(optNext -> {
          cachedPrev = cachedNext;
          cachedNext = optNext;
          return cachedPrev;
        });
      } else {
        lastAdvance = FORWARD;
        return iterator.next().thenCompose(ignored -> iterator.next())
            .thenCompose(optNext -> optNext.isPresent() ? pollNext()
                : CompletableFuture.completedFuture(optNext));
      }
    }

    // public CompletionStage<Void> advanceNext() {
    // assert cachedNext == null;
    // return (lastAdvance == REVERSE ? iterator.next().thenCompose(optPrev -> {
    // cachedPrev = optPrev;
    // return iterator.next();
    // }) : iterator.next()).thenAccept(optNext -> {
    // cachedNext = optNext;
    // lastAdvance = FORWARD;
    // });
    // }
    //
    // public CompletionStage<Void> advancePrev() {
    // assert cachedPrev == null;
    // return (lastAdvance == FORWARD ? iterator.prev().thenCompose(optNext -> {
    // cachedNext = optNext;
    // return iterator.prev();
    // }) : iterator.prev()).thenAccept(optPrev -> {
    // cachedPrev = optPrev;
    // lastAdvance = REVERSE;
    // });
    // }
    //
    // public CompletionStage<Optional<Entry<InternalKey, ByteBuffer>>> pollNext() {
    // assert cachedNext != null;
    // final Optional<Entry<InternalKey, ByteBuffer>> next = cachedNext;
    // if (next.isPresent()) {
    // cachedPrev = next;
    // cachedNext = null;
    // }
    // if (lastAdvance == REVERSE) {
    // return iterator.next().thenApply(ignored -> next);
    // } else {
    // return CompletableFuture.completedFuture(next);
    // }
    // }
    //
    // public CompletionStage<Optional<Entry<InternalKey, ByteBuffer>>> pollPrev() {
    // assert cachedPrev != null;
    // final Optional<Entry<InternalKey, ByteBuffer>> prev = cachedPrev;
    // if (prev.isPresent()) {
    // cachedNext = prev;
    // cachedPrev = null;
    // }
    // if (lastAdvance == FORWARD) {
    // return iterator.prev().thenApply(ignored -> prev);
    // } else {
    // return CompletableFuture.completedFuture(prev);
    // }
    // }

    public static Comparator<OrdinalIterator> smallerNext(
        final Comparator<InternalKey> keyComparator) {
      return (o1, o2) -> {
        assert o1.cachedNext != null;
        assert o2.cachedNext != null;

        if (o1.cachedNext.isPresent()) {
          if (o2.cachedNext.isPresent()) {
            final int result =
                keyComparator.compare(o1.cachedNext.get().getKey(), o2.cachedNext.get().getKey());
            return result == 0 ? Integer.compare(o1.ordinal, o2.ordinal) : result;
          }
          return -1; // o2 does not have a next element, consider o1 smaller than the empty o2
        }
        if (o2.cachedNext.isPresent()) {
          return 1;// o1 does not have a next element, consider o2 smaller than the empty o1
        }
        return 0;// neither o1 nor o2 have a next element, consider them equals as empty iterators
                 // in this direction
      };
    }

    public static Comparator<OrdinalIterator> largerPrev(
        final Comparator<InternalKey> keyComparator) {
      return (o1, o2) -> {
        assert o1.cachedPrev != null;
        assert o2.cachedPrev != null;

        if (o1.cachedPrev.isPresent()) {
          if (o2.cachedPrev.isPresent()) {
            final int result =
                keyComparator.compare(o1.cachedPrev.get().getKey(), o2.cachedPrev.get().getKey());
            return result == 0 ? Integer.compare(o1.ordinal, o2.ordinal) : result;
          }
          return 1;
        }
        if (o2.cachedPrev.isPresent()) {
          return -1;
        }
        return 0;
      };
    }

    @Override
    public String toString() {
      return "Ord" + ordinal + " [" + iterator.toString() + "]";
    }
  }

  @Override
  public String toString() {
    return "MergingIterator [" + Arrays.toString(iters) + "]";
  }
}
