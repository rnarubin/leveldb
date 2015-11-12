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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.testng.Assert;
import org.testng.annotations.Test;

public class CompletableFuturesTest {

  @Test
  public void testComposeUnconditional() throws InterruptedException {
    @SuppressWarnings("serial")
    class DummyException extends RuntimeException {
    }
    final AtomicInteger i = new AtomicInteger(0);
    try {
      CompletableFutures.composeUnconditionally(CompletableFuture.supplyAsync(() -> {
        throw new DummyException();
      }), ignored -> CompletableFuture.supplyAsync(() -> i.compareAndSet(0, 1)))
          .toCompletableFuture().get();
    } catch (final ExecutionException expected) {
      Assert.assertTrue(expected.getCause() instanceof DummyException);
      Assert.assertEquals(i.get(), 1);
    }
    try {
      CompletableFutures
          .composeUnconditionally(CompletableFuture.supplyAsync(() -> i.compareAndSet(1, 2)),
              ignored -> CompletableFuture.supplyAsync(() -> {
                throw new DummyException();
              }))
          .toCompletableFuture().get();
    } catch (final ExecutionException expected) {
      Assert.assertEquals(i.get(), 2);
      Assert.assertTrue(expected.getCause() instanceof DummyException);
    }

    /*
     * TODO something doesn't work with suppressions
     *
     * @SuppressWarnings("serial") class DummySuppressedException extends RuntimeException { }
     *
     * try { CompletableFutures.composeUnconditionally(CompletableFuture.supplyAsync(() -> { throw
     * new DummyException(); }), ignored -> CompletableFuture.supplyAsync(() -> { throw new
     * DummySuppressedException(); })).toCompletableFuture().get(); } catch (final
     * ExecutionException expected) { Assert.assertTrue(expected.getCause() instanceof
     * DummyException); final Throwable[] suppressed = expected.getSuppressed();
     * Assert.assertEquals(suppressed.length, 1); Assert.assertTrue(suppressed[0].getCause()
     * instanceof DummySuppressedException); }
     */
  }

  @Test
  public void testAsyncFilter() throws InterruptedException, ExecutionException {
    final Predicate<String> filter = s -> s.startsWith("a");
    final List<String> unfiltered =
        Arrays.asList("abc", "bat", "alpaca", "dog", "cow", "aardvark", "apple");
    final List<String> expected = unfiltered.stream().filter(filter).collect(Collectors.toList());
    final List<String> actual =
        CompletableFutures
            .filterMapAndCollapse(Iterators.async(unfiltered.iterator()), filter,
                Function.identity(), Runnable::run)
            .toCompletableFuture().get().collect(Collectors.toList());
    Assert.assertEquals(actual, expected);
  }

}


