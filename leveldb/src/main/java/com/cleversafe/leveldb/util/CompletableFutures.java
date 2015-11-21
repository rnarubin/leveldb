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

package com.cleversafe.leveldb.util;

import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.cleversafe.leveldb.AsynchronousCloseable;
import com.cleversafe.leveldb.AsynchronousIterator;
import com.cleversafe.leveldb.util.Iterators.Direction;

public final class CompletableFutures {
  private CompletableFutures() {}

  public static <T, U> CompletionStage<Stream<U>> mapSequential(final Stream<T> stream,
      final Function<T, CompletionStage<U>> mapper) {
    final Iterator<T> iter = stream.iterator();
    if (!iter.hasNext()) {
      return CompletableFuture.completedFuture(Stream.of());
    }
    final Stream.Builder<U> builder = Stream.builder();
    return unrollImmediate(ignored -> iter.hasNext(),
        (final T t) -> mapper.apply(t).thenApply(u -> {
          builder.add(u);
          return t;
        }), iter.next()).thenApply(ignored -> builder.build());
  }

  public static <T, U> CompletionStage<Stream<U>> mapAndCollapse(final AsynchronousIterator<T> iter,
      final Function<T, U> mapper, final Executor asyncExec) {
    return filterMapAndCollapse(iter, always -> true, mapper, asyncExec);
  }

  public static <T, U> CompletionStage<Stream<U>> mapAndCollapse(
      final ReverseAsynchronousIterator<T> iter, final Direction direction,
      final Function<T, U> mapper, final Executor asyncExec) {
    return filterMapAndCollapse(iter, direction, always -> true, mapper, asyncExec);
  }

  public static <T, U> CompletionStage<Stream<U>> filterMapAndCollapse(
      final AsynchronousIterator<T> iter, final Predicate<T> filter, final Function<T, U> mapper,
      final Executor asyncExec) {
    return filterMapAndCollapse(new ReverseAsynchronousIterator<T>() {
      @Override
      public CompletionStage<Optional<T>> next() {
        return iter.next();
      }

      @Override
      public CompletionStage<Optional<T>> prev() {
        throw new UnsupportedOperationException();
      }

    }, Direction.FORWARD, filter, mapper, asyncExec);
  }

  public static <T, U> CompletionStage<Stream<U>> filterMapAndCollapse(
      final ReverseAsynchronousIterator<T> iter, final Direction direction,
      final Predicate<T> filter, final Function<T, U> mapper, final Executor asyncExec) {
    return filterMapCollapseStep(iter, direction, Stream.builder(), filter, mapper, asyncExec);
  }

  private static <T, U> CompletionStage<Stream<U>> filterMapCollapseStep(
      final ReverseAsynchronousIterator<T> iter, final Direction direction,
      final Stream.Builder<U> builder, final Predicate<T> filter, final Function<T, U> mapper,
      final Executor asyncExec) {
    // TODO safe recursion
    return direction.asyncAdvance(iter).thenComposeAsync(
        optNext -> optNext.map(next -> filterMapCollapseStep(iter, direction,
            filter.test(next) ? builder.add(mapper.apply(next)) : builder, filter, mapper,
            asyncExec)).orElseGet(() -> CompletableFuture.completedFuture(builder.build())),
        asyncExec);
  }


  /**
   * used to prevent unbounded stack growth in recursive async calls
   */
  public static <T> CompletionStage<T> unroll(final Predicate<T> whileCondition,
      final ExceptionalFunction<T, CompletionStage<T>> f, final CompletionStage<T> seed) {
    return compose(seed, new CompletableFuture<>(),
        (t, future) -> unrollStep(future, whileCondition, f, t, null, null));
  }


  public static <T> CompletionStage<T> unrollImmediate(final Predicate<T> whileCondition,
      final ExceptionalFunction<T, CompletionStage<T>> f, final T seed) {
    final CompletableFuture<T> future = new CompletableFuture<>();
    unrollStep(future, whileCondition, f, seed, null, null);
    return future;
  }

  private static <T> void unrollStep(final CompletableFuture<T> future,
      final Predicate<T> whileCondition, final ExceptionalFunction<T, CompletionStage<T>> f,
      final T current, final Thread previousThread, final PassBack<T> previousPassBack) {
    final Thread currentThread = Thread.currentThread();

    if (currentThread.equals(previousThread) && previousPassBack.isRunning) {
      previousPassBack.item = current;
    } else {
      final PassBack<T> currentPassBack = new PassBack<>(current);
      while (!currentPassBack.isEmpty()) {
        final T t = currentPassBack.poll();
        try {
          if (whileCondition.test(t)) {
            compose(f.apply(t), future, (next, ignored) -> unrollStep(future, whileCondition, f,
                next, currentThread, currentPassBack));
          } else {
            future.complete(t);
          }
        } catch (final Throwable e) {
          future.completeExceptionally(e);
        }
      }
      currentPassBack.isRunning = false;
    }
  }

  private static class PassBack<T> {
    private static final Object EMPTY = new Object();
    boolean isRunning = true;
    private Object item;

    public PassBack(final T t) {
      this.item = t;
    }

    public boolean isEmpty() {
      return item == EMPTY;
    }

    @SuppressWarnings("unchecked")
    public T poll() {
      assert !isEmpty();
      final Object t = item;
      item = EMPTY;
      return (T) t;
    }

  }

  public static <T> CompletionStage<Stream<T>> allOf(final Stream<CompletionStage<T>> stages) {
    @SuppressWarnings("unchecked")
    final CompletableFuture<T>[] futures =
        stages.map(CompletionStage::toCompletableFuture).toArray(CompletableFuture[]::new);

    return CompletableFuture.allOf(futures)
        .thenApply(voided -> Stream.of(futures).map(CompletableFuture::join));
  }

  public static <T> CompletionStage<Void> allOfVoid(final Stream<CompletionStage<T>> stages) {
    @SuppressWarnings("unchecked")
    final CompletableFuture<T>[] futures =
        stages.map(CompletionStage::toCompletableFuture).toArray(CompletableFuture[]::new);

    return CompletableFuture.allOf(futures);
  }

  public static <T, U> CompletionStage<U> handleExceptional(final CompletionStage<T> first,
      final ExceptionalBiFunction<? super T, Throwable, ? extends U> handler) {
    final CompletableFuture<U> f = new CompletableFuture<>();
    first.whenComplete((t, tException) -> {
      try {
        f.complete(handler.apply(t, tException));
      } catch (final Throwable e) {
        f.completeExceptionally(e);
      }
    });
    return f;
  }

  public static <T, U> CompletableFuture<U> compose(final CompletionStage<T> first,
      final CompletableFuture<U> then,
      final ExceptionalBiConsumer<T, CompletableFuture<U>> onSuccess) {
    first.whenComplete((success, exception) -> {
      if (exception != null) {
        then.completeExceptionally(exception);
      } else {
        try {
          onSuccess.accept(success, then);
        } catch (final Throwable t) {
          then.completeExceptionally(t);
        }
      }
    });
    return then;
  }

  public static <T> CompletableFuture<T> compose(final CompletionStage<T> first,
      final CompletableFuture<T> then) {
    return compose(first, then, (t, future) -> future.complete(t));
  }

  public static <T, U> CompletionStage<U> thenApplyExceptional(final CompletionStage<T> first,
      final ExceptionalFunction<? super T, ? extends U> applier) {
    return compose(first, new CompletableFuture<>(),
        (t, future) -> future.complete(applier.apply(t)));
  }

  public static <T, U> CompletionStage<U> thenComposeExceptional(final CompletionStage<T> first,
      final ExceptionalFunction<? super T, ? extends CompletionStage<U>> composer) {
    return compose(first, new CompletableFuture<>(),
        (t, future) -> compose(composer.apply(t), future));
  }

  public static <T> CompletionStage<T> composeOnException(final CompletionStage<T> first,
      final ExceptionalFunction<Throwable, CompletionStage<Void>> onException) {
    final CompletableFuture<T> f = new CompletableFuture<>();
    first.whenComplete((t, tException) -> {
      if (tException != null) {
        try {
          onException.apply(tException).whenComplete((voided, suppressedException) -> {
            if (suppressedException != null) {
              tException.addSuppressed(suppressedException);
            }
            f.completeExceptionally(tException);
          });
        } catch (final Throwable e) {
          f.completeExceptionally(e);
        }
      } else {
        f.complete(t);
      }
    });
    return f;
  }

  /**
   * Like {@link CompletionStage#thenCompose(Function)} except that the second stage is always
   * executed on the completion of the first stage (normal or exceptional). If the first stage
   * completes with an exception, the second stage will be run (with an {@link Optional#empty()
   * empty} argument), but the returned stage will fail upon the second's completion with the first
   * stage's exception. If both the first and the second stage complete with exceptions, the
   * returned stage will fail with the first stage's exception
   */
  public static <T, U> CompletionStage<U> composeUnconditionally(final CompletionStage<T> first,
      final ExceptionalFunction<Optional<T>, ? extends CompletionStage<U>> then) {
    final CompletableFuture<U> f = new CompletableFuture<U>();
    first.whenComplete((t, tException) -> {
      try {
        then.apply(Optional.ofNullable(t)).whenComplete((u, uException) -> {
          if (tException != null) {
            if (uException != null) {
              // TODO this suppression doesn't seem to propagate
              tException.addSuppressed(uException);
            }
            f.completeExceptionally(tException);
          } else if (uException != null) {
            f.completeExceptionally(uException);
          } else {
            f.complete(u);
          }
        });
      } catch (final Throwable e) {
        f.completeExceptionally(e);
      }
    });
    return f;
  }

  public static <T> CompletionStage<T> closeAfter(final CompletionStage<T> first,
      final AsynchronousCloseable closeable) {
    return composeUnconditionally(first,
        optResult -> closeable.asyncClose().thenApply(closed -> optResult.orElse(null)));
  }

  public static <T> CompletableFuture<T> exceptionalFuture(final Throwable ex) {
    final CompletableFuture<T> f = new CompletableFuture<>();
    f.completeExceptionally(ex);
    return f;
  }

  @FunctionalInterface
  public interface ExceptionalBiFunction<T, U, R> {
    R apply(final T t, final U u) throws Throwable;
  }

  @FunctionalInterface
  public interface ExceptionalFunction<T, R> {
    R apply(final T t) throws Throwable;
  }

  @FunctionalInterface
  public interface ExceptionalBiConsumer<T, U> {
    void accept(final T t, final U u) throws Throwable;
  }

}
