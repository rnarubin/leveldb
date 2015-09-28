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

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Stream;

import org.iq80.leveldb.AsynchronousIterator;
import org.iq80.leveldb.ReverseAsynchronousIterator;
import org.iq80.leveldb.util.Iterators.Direction;

public final class CompletableFutures {
  private CompletableFutures() {}

  public static <T, U> CompletionStage<Stream<U>> flatMapIterator(
      final AsynchronousIterator<T> iter, final Function<T, U> mapper, final Executor asyncExec) {
    return flatMapIterator(new ReverseAsynchronousIterator<T>() {
      @Override
      public CompletionStage<Optional<T>> next() {
        return iter.next();
      }

      @Override
      public CompletionStage<Optional<T>> prev() {
        throw new UnsupportedOperationException();
      }

    }, Direction.NEXT, mapper, asyncExec);
  }

  public static <T, U> CompletionStage<Stream<U>> flatMapIterator(
      final ReverseAsynchronousIterator<T> iter, final Direction direction,
      final Function<T, U> mapper, final Executor asyncExec) {
    return iteratorStep(iter, direction, Stream.builder(), mapper, asyncExec);
  }

  private static <T, U> CompletionStage<Stream<U>> iteratorStep(
      final ReverseAsynchronousIterator<T> iter, final Direction direction,
      final Stream.Builder<U> builder, final Function<T, U> mapper, final Executor asyncExec) {
    return direction.asyncAdvance(iter).thenComposeAsync(next -> {
      if (next.isPresent()) {
        builder.add(mapper.apply(next.get()));
        return iteratorStep(iter, direction, builder, mapper, asyncExec);
      } else {
        return CompletableFuture.completedFuture(builder.build());
      }
    } , asyncExec);
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

  public static <T, U> CompletionStage<U> handleExceptionally(final CompletionStage<T> first,
      final ExceptionalBiFunction<? super T, Throwable, ? extends U> handler) {
    final CompletableFuture<U> f = new CompletableFuture<>();
    first.whenComplete((t, throwable) -> {
      try {
        f.complete(handler.apply(t, throwable));
      } catch (final Exception e) {
        f.completeExceptionally(e);
      }
    });
    return f;
  }

  public static <T, U> CompletionStage<U> applyExceptionally(final CompletionStage<T> first,
      final ExceptionalFunction<? super T, ? extends U> applier) {
    final CompletableFuture<U> f = new CompletableFuture<>();
    first.whenComplete((t, throwable) -> {
      if (throwable != null) {
        f.completeExceptionally(throwable);
      } else {
        try {
          f.complete(applier.apply(t));
        } catch (final Exception e) {
          f.completeExceptionally(e);
        }
      }
    });
    return f;
  }

  public static <T, U> CompletionStage<U> composeExceptionally(final CompletionStage<T> first,
      final ExceptionalFunction<? super T, ? extends CompletionStage<U>> composer) {
    final CompletableFuture<U> f = new CompletableFuture<>();
    first.whenComplete((t, tException) -> {
      if (tException != null) {
        f.completeExceptionally(tException);
      } else {
        try {
          composer.apply(t).whenComplete((u, uException) -> {
            if (uException != null) {
              f.completeExceptionally(uException);
            } else {
              f.complete(u);
            }
          });
        } catch (final Exception e) {
          f.completeExceptionally(e);
        }
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
   * returned stage will fail with the first stage's exception and the second stage's exception
   * added as a suppressed exception
   * <p>
   * Think of this as an async "try/finally" where the first stage is within the "try" block and the
   * second stage is within the "finally"
   */
  public static <T, U> CompletionStage<U> composeUnconditionally(final CompletionStage<T> first,
      final Function<Optional<T>, ? extends CompletionStage<U>> then) {
    final CompletableFuture<U> f = new CompletableFuture<U>();
    first.whenComplete((t, tException) -> {
      then.apply(Optional.ofNullable(t)).whenComplete((u, uException) -> {
        if (tException != null) {
          if (uException != null) {
            tException.addSuppressed(uException);
          }
          f.completeExceptionally(tException);
        } else if (uException != null) {
          f.completeExceptionally(uException);
        } else {
          f.complete(u);
        }
      });
    });
    return f;
  }

  public static <T> CompletableFuture<T> exceptionalFuture(final Throwable ex) {
    final CompletableFuture<T> f = new CompletableFuture<>();
    f.completeExceptionally(ex);
    return f;
  }

  @FunctionalInterface
  public interface ExceptionalBiFunction<T, U, R> {
    R apply(final T t, final U u) throws Exception;
  }

  @FunctionalInterface
  public interface ExceptionalFunction<T, R> {
    R apply(final T t) throws Exception;
  }

}
