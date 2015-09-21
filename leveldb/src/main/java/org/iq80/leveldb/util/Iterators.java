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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.iq80.leveldb.ReverseAsynchronousIterator;
import org.iq80.leveldb.SeekingAsynchronousIterator;
import org.iq80.leveldb.impl.ReverseIterator;
import org.iq80.leveldb.impl.ReverseSeekingIterator;

public final class Iterators {
  private Iterators() {}

  public static enum Direction {
    /*
     * reversable iterators don't have a consistent concept of a "current" item. instead they exist
     * in a position "between" next and prev. in order to make the data iterator 'current' work, we
     * need to track from which direction it was initialized so that calls to advance the
     * encompassing index iterator are consistent
     */
    NEXT {
      @Override
      public boolean hasMore(final ReverseIterator<?> iter) {
        return iter.hasNext();
      }

      @Override
      public <T> T advance(final ReverseIterator<T> iter) {
        return iter.next();
      }

      @Override
      public <T> CompletionStage<Optional<T>> asyncAdvance(
          final ReverseAsynchronousIterator<T> iter) {
        return iter.next();
      }

      @Override
      public CompletionStage<Void> seekToEdge(final SeekingAsynchronousIterator<?, ?> iter) {
        return iter.seekToFirst();
      }
    },
    PREV {
      @Override
      public boolean hasMore(final ReverseIterator<?> iter) {
        return iter.hasPrev();
      }

      @Override
      public <T> T advance(final ReverseIterator<T> iter) {
        return iter.prev();
      }

      @Override
      public <T> CompletionStage<Optional<T>> asyncAdvance(
          final ReverseAsynchronousIterator<T> iter) {
        return iter.prev();
      }

      @Override
      public CompletionStage<Void> seekToEdge(final SeekingAsynchronousIterator<?, ?> iter) {
        return iter.seekToEnd();
      }
    };

    public abstract boolean hasMore(ReverseIterator<?> iter);

    public abstract <T> T advance(ReverseIterator<T> iter);

    public abstract <T> CompletionStage<Optional<T>> asyncAdvance(
        ReverseAsynchronousIterator<T> iter);

    public abstract CompletionStage<Void> seekToEdge(SeekingAsynchronousIterator<?, ?> iter);
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
  }
}
