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

package com.cleversafe.leveldb;

import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import com.cleversafe.leveldb.util.CompletableFutures;
import com.google.common.collect.Maps;

/**
 * An iterator which can be advanced asynchronously (wherein calls to {@link #next} return a
 * {@link CompletionStage})
 * <p>
 * Note: this interface does <i>not</i> guarantee thread-safe traversal. Operations should be
 * performed in stage-sequential order.
 * <p>
 * For example, the first two items of the iterator may be accessed like so:
 *
 * <pre>
 * CompletionStage<T> first = iter.next();
 * CompletionStage<T> second = first.thenCompose(f -> iter.next());
 * </pre>
 *
 *
 * The following example is <b>not correct</b> and may yield undefined results:
 *
 * <pre>
 * CompletionStage<T> first = iter.next();
 * CompletionStage<T> second = iter.next(); // did not follow the first stage!
 * </pre>
 */
public interface AsynchronousIterator<T> {
  /**
   * Advances the iterator, returning an {@link Optional} containing the next element, or
   * {@link Optional#empty()} if there are no more elements
   */
  CompletionStage<Optional<T>> next();

  /**
   * @return an iterator consisting of the results of applying the given function to the elements of
   *         this iterator.
   */
  default <R> AsynchronousIterator<R> map(final Function<? super T, ? extends R> mapper) {
    return () -> next().thenApply(optT -> optT.map(mapper));
  }

  /**
   * @return an iterator consisting of the elements of this iterator that match the given predicate.
   */
  default AsynchronousIterator<T> filter(final Predicate<? super T> predicate) {
    return () -> CompletableFutures.unroll(optT -> optT.filter(predicate.negate()).isPresent(),
        ignored -> next(), next());
  }

  /**
   * @return a reduction of the elements of this iterator performed using the given identity and
   *         accumulator function
   */
  default <U> CompletionStage<U> reduce(final U seed,
      final BiFunction<U, ? super T, U> accumulator) {
    return CompletableFutures
        .<Entry<Optional<T>, U>>unroll(pair -> pair.getKey().isPresent(),
            pair -> next().thenApply(optNext -> Maps.immutableEntry(optNext,
                accumulator.apply(pair.getValue(), pair.getKey().get()))),
        next().thenApply(optNext -> Maps.immutableEntry(optNext, seed)))
        .thenApply(Entry::getValue);
  }
}
