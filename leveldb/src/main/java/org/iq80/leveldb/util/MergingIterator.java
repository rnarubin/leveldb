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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.iq80.leveldb.SeekingAsynchronousIterator;
import org.iq80.leveldb.impl.InternalKey;

public final class MergingIterator implements SeekingAsynchronousIterator<InternalKey, ByteBuffer> {
  private final OrdinalIterator[] iters;
  private final Comparator<OrdinalIterator> smallerNext, largerPrev;
  private final Comparator<InternalKey> internalKeyComparator;

  private MergingIterator(
      final List<SeekingAsynchronousIterator<InternalKey, ByteBuffer>> iterators,
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
    this.internalKeyComparator = internalKeyComparator;
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
    return CompletableFutures
        .allOfVoid(Stream.of(iters).filter(iter -> iter.peekedNext == null)
            .map(OrdinalIterator::advanceNext))
        // all iterators will now have a peek
        .thenApply(voided -> Stream.of(iters).min(smallerNext).flatMap(OrdinalIterator::pollNext));
  }

  @Override
  public CompletionStage<Optional<Entry<InternalKey, ByteBuffer>>> prev() {
    return CompletableFutures
        .allOfVoid(Stream.of(iters).filter(iter -> iter.peekedPrev == null)
            .map(OrdinalIterator::advancePrev))
        .thenApply(voided -> Stream.of(iters).min(largerPrev).flatMap(OrdinalIterator::pollPrev));
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
    return CompletableFutures.allOfVoid(Stream.of(iters).map(ord -> ord.seek(key)));
  }

  // TODO(optimization) heap, overlap seek and read
  private static final class OrdinalIterator {
    private final SeekingAsynchronousIterator<InternalKey, ByteBuffer> iterator;
    private final int ordinal;
    private Optional<Entry<InternalKey, ByteBuffer>> peekedNext, peekedPrev;

    public OrdinalIterator(final int ordinal,
        final SeekingAsynchronousIterator<InternalKey, ByteBuffer> iterator) {
      this.ordinal = ordinal;
      this.iterator = iterator;
    }

    public CompletionStage<Void> seekToFirst() {
      // nullable optional, i know, it feels so wrong
      peekedNext = peekedPrev = null;
      return iterator.seekToFirst();
    }

    public CompletionStage<Void> seekToEnd() {
      peekedNext = peekedPrev = null;
      return iterator.seekToEnd();
    }

    public CompletionStage<Void> seek(final InternalKey key) {
      peekedNext = peekedPrev = null;
      return iterator.seek(key);
    }

    public CompletionStage<Void> asyncClose() {
      return iterator.asyncClose();
    }

    public CompletionStage<Void> advanceNext() {
      assert peekedNext == null;
      return iterator.next().thenAccept(optNext -> peekedNext = optNext);
    }

    public Optional<Entry<InternalKey, ByteBuffer>> pollNext() {
      final Optional<Entry<InternalKey, ByteBuffer>> next = peekedNext;
      peekedPrev = next;
      peekedNext = null;
      return next;
    }

    public CompletionStage<Void> advancePrev() {
      assert peekedPrev == null;
      return iterator.prev().thenAccept(optPrev -> peekedPrev = optPrev);
    }

    public Optional<Entry<InternalKey, ByteBuffer>> pollPrev() {
      final Optional<Entry<InternalKey, ByteBuffer>> prev = peekedPrev;
      peekedNext = prev;
      peekedPrev = null;
      return prev;
    }

    public static Comparator<OrdinalIterator> smallerNext(
        final Comparator<InternalKey> keyComparator) {
      return (o1, o2) -> {
        assert o1.peekedNext != null;
        assert o2.peekedNext != null;

        if (o1.peekedNext.isPresent()) {
          if (o2.peekedNext.isPresent()) {
            final int result =
                keyComparator.compare(o1.peekedNext.get().getKey(), o2.peekedNext.get().getKey());
            return result == 0 ? Integer.compare(o1.ordinal, o2.ordinal) : result;
          }
          return -1; // o2 does not have a next element, consider o1 smaller than the empty o2
        }
        if (o2.peekedNext.isPresent()) {
          return 1;// o1 does not have a next element, consider o2 smaller than the empty o1
        }
        return 0;// neither o1 nor o2 have a next element, consider them equals as empty iterators
                 // in this direction
      };
    }

    public static Comparator<OrdinalIterator> largerPrev(
        final Comparator<InternalKey> keyComparator) {
      return (o1, o2) -> {
        assert o1.peekedPrev != null;
        assert o2.peekedPrev != null;

        if (o1.peekedPrev.isPresent()) {
          if (o2.peekedPrev.isPresent()) {
            final int result =
                keyComparator.compare(o1.peekedPrev.get().getKey(), o2.peekedPrev.get().getKey());
            return result == 0 ? Integer.compare(o1.ordinal, o2.ordinal) : result;
          }
          return 1;
        }
        if (o2.peekedPrev.isPresent()) {
          return -1;
        }
        return 0;
      };
    }
  }

  @Override
  public String toString() {
    return "MergingIterator [iterators=" + Arrays.toString(iters) + ", comparator="
        + internalKeyComparator + "]";
  }
}
