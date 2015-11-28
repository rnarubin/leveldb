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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class CompletedStage<T> implements CompletionStage<T> {

  private static final Executor EXEC = ForkJoinPool.getCommonPoolParallelism() > 1
      ? ForkJoinPool.commonPool() : runnable -> new Thread(runnable).start();
  public static final CompletedStage<Void> VOID = of(null);

  private final T result;
  private final Throwable exception;

  private CompletedStage(final T result, final Throwable exception) {
    this.result = result;
    this.exception = exception;
  }

  public static <T> CompletedStage<T> of(final T result) {
    return new CompletedStage<>(result, null);
  }

  public static <T> CompletedStage<T> exception(final Throwable exception) {
    return new CompletedStage<>(null, exception);
  }

  // cheat the generics to save an object instantiation
  // @SuppressWarnings("unchecked")
  private <U> CompletedStage<U> thisException() {
    assert exception != null;
    // return (CompletedStage<U>) this;
    return exception(exception);
  }

  @Override
  public <U> CompletionStage<U> thenApply(final Function<? super T, ? extends U> fn) {
    try {
      return exception == null ? of(fn.apply(result)) : thisException();
    } catch (final Throwable t) {
      return exception(t);
    }
  }

  @Override
  public <U> CompletionStage<U> thenApplyAsync(final Function<? super T, ? extends U> fn) {
    return thenApplyAsync(fn, EXEC);
  }

  @Override
  public <U> CompletionStage<U> thenApplyAsync(final Function<? super T, ? extends U> fn,
      final Executor executor) {
    return exception == null ? CompletableFuture.supplyAsync(() -> fn.apply(result), executor)
        : thisException();
  }

  @Override
  public CompletionStage<Void> thenAccept(final Consumer<? super T> action) {
    try {
      if (exception == null) {
        action.accept(result);
        return VOID;
      } else {
        return thisException();
      }
    } catch (final Throwable t) {
      return exception(t);
    }
  }

  @Override
  public CompletionStage<Void> thenAcceptAsync(final Consumer<? super T> action) {
    return thenAcceptAsync(action, EXEC);
  }

  @Override
  public CompletionStage<Void> thenAcceptAsync(final Consumer<? super T> action,
      final Executor executor) {
    return exception == null ? CompletableFuture.runAsync(() -> action.accept(result), executor)
        : thisException();
  }

  @Override
  public CompletionStage<Void> thenRun(final Runnable action) {
    try {
      if (exception == null) {
        action.run();
        return VOID;
      } else {
        return thisException();
      }
    } catch (final Throwable t) {
      return exception(t);
    }
  }

  @Override
  public CompletionStage<Void> thenRunAsync(final Runnable action) {
    return thenRunAsync(action, EXEC);
  }

  @Override
  public CompletionStage<Void> thenRunAsync(final Runnable action, final Executor executor) {
    return exception == null ? CompletableFuture.runAsync(action, executor) : thisException();
  }

  @Override
  public <U, V> CompletionStage<V> thenCombine(final CompletionStage<? extends U> other,
      final BiFunction<? super T, ? super U, ? extends V> fn) {
    return exception == null ? other.thenApply(u -> fn.apply(result, u)) : thisException();
  }

  @Override
  public <U, V> CompletionStage<V> thenCombineAsync(final CompletionStage<? extends U> other,
      final BiFunction<? super T, ? super U, ? extends V> fn) {
    return thenCombineAsync(other, fn, EXEC);
  }

  @Override
  public <U, V> CompletionStage<V> thenCombineAsync(final CompletionStage<? extends U> other,
      final BiFunction<? super T, ? super U, ? extends V> fn, final Executor executor) {
    return exception == null ? other.thenApplyAsync(u -> fn.apply(result, u)) : thisException();
  }

  @Override
  public <U> CompletionStage<Void> thenAcceptBoth(final CompletionStage<? extends U> other,
      final BiConsumer<? super T, ? super U> action) {
    return exception == null ? other.thenAccept(u -> action.accept(result, u)) : thisException();
  }

  @Override
  public <U> CompletionStage<Void> thenAcceptBothAsync(final CompletionStage<? extends U> other,
      final BiConsumer<? super T, ? super U> action) {
    return thenAcceptBothAsync(other, action, EXEC);
  }

  @Override
  public <U> CompletionStage<Void> thenAcceptBothAsync(final CompletionStage<? extends U> other,
      final BiConsumer<? super T, ? super U> action, final Executor executor) {
    return exception == null ? other.thenAcceptAsync(u -> action.accept(result, u))
        : thisException();
  }

  @Override
  public CompletionStage<Void> runAfterBoth(final CompletionStage<?> other, final Runnable action) {
    return exception == null ? other.thenRun(action) : thisException();
  }

  @Override
  public CompletionStage<Void> runAfterBothAsync(final CompletionStage<?> other,
      final Runnable action) {
    return runAfterBothAsync(other, action, EXEC);
  }

  @Override
  public CompletionStage<Void> runAfterBothAsync(final CompletionStage<?> other,
      final Runnable action, final Executor executor) {
    return exception == null ? other.thenRunAsync(action, executor) : thisException();
  }

  @Override
  public <U> CompletionStage<U> applyToEither(final CompletionStage<? extends T> other,
      final Function<? super T, U> fn) {
    return thenApply(fn);
  }

  @Override
  public <U> CompletionStage<U> applyToEitherAsync(final CompletionStage<? extends T> other,
      final Function<? super T, U> fn) {
    return thenApplyAsync(fn);
  }

  @Override
  public <U> CompletionStage<U> applyToEitherAsync(final CompletionStage<? extends T> other,
      final Function<? super T, U> fn, final Executor executor) {
    return thenApplyAsync(fn, executor);
  }

  @Override
  public CompletionStage<Void> acceptEither(final CompletionStage<? extends T> other,
      final Consumer<? super T> action) {
    return thenAccept(action);
  }

  @Override
  public CompletionStage<Void> acceptEitherAsync(final CompletionStage<? extends T> other,
      final Consumer<? super T> action) {
    return thenAcceptAsync(action);
  }

  @Override
  public CompletionStage<Void> acceptEitherAsync(final CompletionStage<? extends T> other,
      final Consumer<? super T> action, final Executor executor) {
    return thenAcceptAsync(action, executor);
  }

  @Override
  public CompletionStage<Void> runAfterEither(final CompletionStage<?> other,
      final Runnable action) {
    return thenRun(action);
  }

  @Override
  public CompletionStage<Void> runAfterEitherAsync(final CompletionStage<?> other,
      final Runnable action) {
    return thenRunAsync(action);
  }

  @Override
  public CompletionStage<Void> runAfterEitherAsync(final CompletionStage<?> other,
      final Runnable action, final Executor executor) {
    return thenRunAsync(action, executor);
  }

  @Override
  public <U> CompletionStage<U> thenCompose(
      final Function<? super T, ? extends CompletionStage<U>> fn) {
    try {
      return fn.apply(result);
    } catch (final Throwable t) {
      return exception(t);
    }
  }

  @Override
  public <U> CompletionStage<U> thenComposeAsync(
      final Function<? super T, ? extends CompletionStage<U>> fn) {
    return thenComposeAsync(fn, EXEC);
  }

  @Override
  public <U> CompletionStage<U> thenComposeAsync(
      final Function<? super T, ? extends CompletionStage<U>> fn, final Executor executor) {
    try {
      if (exception == null) {
        final CompletableFuture<U> f = new CompletableFuture<>();
        executor.execute(() -> CompletableFutures.compose(fn.apply(result), f));
        return f;
      } else {
        return thisException();
      }
    } catch (final Throwable t) {
      return exception(t);
    }
  }

  @Override
  public CompletionStage<T> exceptionally(final Function<Throwable, ? extends T> fn) {
    try {
      if (exception == null) {
        return this;
      } else {
        return of(fn.apply(exception));
      }
    } catch (final Throwable t) {
      return exception(t);
    }
  }

  @Override
  public CompletionStage<T> whenComplete(final BiConsumer<? super T, ? super Throwable> action) {
    try {
      action.accept(result, exception);
      return this;
    } catch (final Throwable t) {
      return exception(t);
    }
  }

  @Override
  public CompletionStage<T> whenCompleteAsync(
      final BiConsumer<? super T, ? super Throwable> action) {
    return whenCompleteAsync(action, EXEC);
  }

  @Override
  public CompletionStage<T> whenCompleteAsync(final BiConsumer<? super T, ? super Throwable> action,
      final Executor executor) {
    try {
      final CompletableFuture<T> f = new CompletableFuture<>();
      executor.execute(() -> {
        try {
          action.accept(result, exception);
          if (exception == null) {
            f.complete(result);
          } else {
            f.completeExceptionally(exception);
          }
        } catch (final Throwable t) {
          f.completeExceptionally(t);
        }
      });
      return f;
    } catch (final Throwable t) {
      return exception(t);
    }
  }

  @Override
  public <U> CompletionStage<U> handle(final BiFunction<? super T, Throwable, ? extends U> fn) {
    try {
      return of(fn.apply(result, exception));
    } catch (final Throwable t) {
      return exception(t);
    }
  }

  @Override
  public <U> CompletionStage<U> handleAsync(
      final BiFunction<? super T, Throwable, ? extends U> fn) {
    return handleAsync(fn, EXEC);
  }

  @Override
  public <U> CompletionStage<U> handleAsync(final BiFunction<? super T, Throwable, ? extends U> fn,
      final Executor executor) {
    try {
      final CompletableFuture<U> f = new CompletableFuture<>();
      executor.execute(() -> {
        try {
          f.complete(fn.apply(result, exception));
        } catch (final Throwable t) {
          f.completeExceptionally(t);
        }
      });
      return f;
    } catch (final Throwable t) {
      return exception(t);
    }
  }

  @Override
  public CompletableFuture<T> toCompletableFuture() {
    if (exception == null) {
      return CompletableFuture.completedFuture(result);
    } else {
      final CompletableFuture<T> f = new CompletableFuture<>();
      f.completeExceptionally(exception);
      return f;
    }
  }
}
