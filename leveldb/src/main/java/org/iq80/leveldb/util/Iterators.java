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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.iq80.leveldb.AsynchronousIterator;
import org.iq80.leveldb.ReverseAsynchronousIterator;
import org.iq80.leveldb.SeekingAsynchronousIterator;
import org.iq80.leveldb.impl.ReverseIterator;
import org.iq80.leveldb.impl.ReversePeekingIterator;
import org.iq80.leveldb.impl.ReverseSeekingIterator;

import com.google.common.base.Preconditions;
import com.google.common.collect.PeekingIterator;

public final class Iterators {
  private Iterators() {}

  public static <T> AsynchronousIterator<T> emptyAsyncIterator() {
    return new EmptyAsyncIterator<T>();
  }

  public static <K, V> SeekingAsynchronousIterator<K, V> emptySeekingAsyncIterator() {
    return new EmptySeekingAsyncIterator<K, V>();
  }

  public static <T> ListReverseIterator<T> listReverseIterator(final ListIterator<T> listIter) {
    return new ListReverseIterator<T>(listIter);
  }

  public static <T> ListReverseIterator<T> listReverseIterator(final List<T> list) {
    return listReverseIterator(list.listIterator());
  }

  public static <T> ListReverseIterator<T> listReverseIterator(final Collection<T> collection) {
    return listReverseIterator(new ArrayList<>(collection));
  }

  public static <T> ReversePeekingIterator<T> reversePeekingIterator(
      final ListIterator<? extends T> listIter) {
    return reversePeekingIterator(Iterators.listReverseIterator(listIter));
  }

  public static <T> ReversePeekingIterator<T> reversePeekingIterator(final List<? extends T> list) {
    return reversePeekingIterator(Iterators.listReverseIterator(list));
  }

  public static <T> ReversePeekingIterator<T> reversePeekingIterator(
      final Collection<? extends T> collection) {
    return reversePeekingIterator(Iterators.listReverseIterator(collection));
  }

  public static <T> ReversePeekingIterator<T> reversePeekingIterator(
      final ReverseIterator<? extends T> iterator) {
    return new ReversePeekingImpl<T>(iterator);
  }

  /**
   * NOTE: current implementation is fairly naive, should only be used in testing
   */
  public static <K, V> ReverseSeekingIterator<K, V> reverseSeekingIterator(
      final List<Entry<K, V>> list, final Comparator<K> comparator) {
    return new NaiveReverseSeekingImpl<>(listReverseIterator(list), comparator);
  }

  private static class ListReverseIterator<E> implements ReverseIterator<E> {
    private final ListIterator<E> iter;

    public ListReverseIterator(final ListIterator<E> listIterator) {
      this.iter = listIterator;
    }

    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public E next() {
      return iter.next();
    }

    @Override
    public void remove() {
      iter.remove();
    }

    @Override
    public E prev() {
      return iter.previous();
    }

    @Override
    public boolean hasPrev() {
      return iter.hasPrevious();
    }
  }

  private static class ReversePeekingImpl<E>
      implements PeekingIterator<E>, ReversePeekingIterator<E> {
    private final ReverseIterator<? extends E> rIterator;
    private boolean rHasPeeked;
    private E rPeekedElement;
    private boolean hasPeeked;
    private E peekedElement;

    ReversePeekingImpl(final ReverseIterator<? extends E> iterator) {
      this.rIterator = Preconditions.checkNotNull(iterator);
    }

    @Override
    public boolean hasNext() {
      return hasPeeked || rIterator.hasNext();
    }

    @Override
    public boolean hasPrev() {
      return rHasPeeked || rIterator.hasPrev();
    }

    @Override
    public E next() {
      hasPeeked = false;
      peekedElement = null;
      final E next = rIterator.next();
      rHasPeeked = true;
      rPeekedElement = next;
      return next;
    }

    @Override
    public E prev() {
      rHasPeeked = false;
      rPeekedElement = null;
      final E prev = rIterator.prev();
      hasPeeked = true;
      peekedElement = prev;
      return prev;
    }

    @Override
    public E peek() {
      if (!hasPeeked) {
        peekedElement = rIterator.next();
        rIterator.prev();
        hasPeeked = true;
      }
      return peekedElement;
    }

    @Override
    public E peekPrev() {
      if (!rHasPeeked) {
        rPeekedElement = rIterator.prev();
        rIterator.next(); // reset to original position
        rHasPeeked = true;
      }
      return rPeekedElement;
    }

    @Override
    public void remove() {
      Preconditions.checkState(!(hasPeeked || rHasPeeked),
          "Can't remove after peeking at next or previous");
      rIterator.remove();
    }
  }

  private static class NaiveReverseSeekingImpl<K, V> extends ReversePeekingImpl<Entry<K, V>>
      implements ReverseSeekingIterator<K, V> {
    private final Comparator<K> comparator;

    public NaiveReverseSeekingImpl(final ReverseIterator<? extends Entry<K, V>> iterator,
        final Comparator<K> comparator) {
      super(iterator);
      this.comparator = comparator;
    }

    @Override
    public void seekToFirst() {
      while (hasPrev()) {
        prev();
      }
    }

    @Override
    public void seekToEnd() {
      while (hasNext()) {
        next();
      }
    }

    @Override
    public void seek(final K targetKey) {
      seekToFirst();
      while (hasNext() && comparator.compare(peek().getKey(), targetKey) < 0) {
        next();
      }
    }
  }

  public static enum Direction {
    FORWARD {
      @Override
      public final boolean hasMore(final ReverseIterator<?> iter) {
        return iter.hasNext();
      }

      @Override
      public final <T> T advance(final ReverseIterator<T> iter) {
        return iter.next();
      }

      @Override
      public final <T> CompletionStage<Optional<T>> asyncAdvance(
          final ReverseAsynchronousIterator<T> iter) {
        return iter.next();
      }

      @Override
      public final CompletionStage<Void> seekToEdge(final SeekingAsynchronousIterator<?, ?> iter) {
        return iter.seekToFirst();
      }
    },
    REVERSE {
      @Override
      public final boolean hasMore(final ReverseIterator<?> iter) {
        return iter.hasPrev();
      }

      @Override
      public final <T> T advance(final ReverseIterator<T> iter) {
        return iter.prev();
      }

      @Override
      public final <T> CompletionStage<Optional<T>> asyncAdvance(
          final ReverseAsynchronousIterator<T> iter) {
        return iter.prev();
      }

      @Override
      public final CompletionStage<Void> seekToEdge(final SeekingAsynchronousIterator<?, ?> iter) {
        return iter.seekToEnd();
      }
    };

    public abstract boolean hasMore(ReverseIterator<?> iter);

    public abstract <T> T advance(ReverseIterator<T> iter);

    public abstract <T> CompletionStage<Optional<T>> asyncAdvance(
        ReverseAsynchronousIterator<T> iter);

    public abstract CompletionStage<Void> seekToEdge(SeekingAsynchronousIterator<?, ?> iter);

    public static Direction opposite(final Direction direction) {
      switch (direction) {
        case FORWARD:
          return REVERSE;
        case REVERSE:
          return FORWARD;
        default:
          throw new IllegalArgumentException("Not a valid direction:" + direction);
      }
    }
  }

  public static class AsyncWrappedSeekingIterator<K, V>
      implements SeekingAsynchronousIterator<K, V> {

    private final ReverseSeekingIterator<K, V> iter;

    public AsyncWrappedSeekingIterator(final ReverseSeekingIterator<K, V> iter) {
      this.iter = iter;
    }

    @Override
    public CompletionStage<Optional<Entry<K, V>>> prev() {
      return CompletableFuture
          .completedFuture(iter.hasPrev() ? Optional.of(iter.prev()) : Optional.empty());
    }

    @Override
    public CompletionStage<Optional<Entry<K, V>>> next() {
      return CompletableFuture
          .completedFuture(iter.hasNext() ? Optional.of(iter.next()) : Optional.empty());
    }

    @Override
    public CompletionStage<Void> seek(final K key) {
      iter.seek(key);
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> seekToFirst() {
      iter.seekToFirst();
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> seekToEnd() {
      iter.seekToEnd();
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> asyncClose() {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public String toString() {
      return "AsyncWrapped [" + iter + "]";
    }
  }

  private static class EmptyAsyncIterator<T> implements AsynchronousIterator<T> {
    @Override
    public CompletionStage<Optional<T>> next() {
      return CompletableFuture.completedFuture(Optional.empty());
    }
  }

  private static class EmptySeekingAsyncIterator<K, V> extends EmptyAsyncIterator<Entry<K, V>>
      implements SeekingAsynchronousIterator<K, V> {
    @Override
    public CompletionStage<Optional<Entry<K, V>>> prev() {
      return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletionStage<Void> asyncClose() {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> seek(final K key) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> seekToFirst() {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> seekToEnd() {
      return CompletableFuture.completedFuture(null);
    }

  }
}
