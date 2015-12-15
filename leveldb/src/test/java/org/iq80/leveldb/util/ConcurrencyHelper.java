/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.iq80.leveldb.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class ConcurrencyHelper<T>
        implements AutoCloseable
{
    ExecutorService threadPool;

    public ConcurrencyHelper(int threadCount)
    {
        this(threadCount, "unnamed-concurrency-helper");
    }

    public ConcurrencyHelper(int threads, String threadPrefix)
    {
        threadPool = Executors.newFixedThreadPool(Math.max(1, threads),
                new ThreadFactoryBuilder().setNameFormat(threadPrefix + "-%d").build());
    }

    public List<Future<T>> submitAll(Collection<Callable<T>> callables)
    {
        List<Future<T>> pendingWork = new ArrayList<>(callables.size());
        for (Callable<T> work : callables) {
            pendingWork.add(threadPool.submit(work));
        }

        return pendingWork;
    }

    public List<T> submitAllAndWait(Collection<Callable<T>> callables, long timeOut, TimeUnit timeUnit)
            throws InterruptedException, ExecutionException
    {
        List<Future<T>> invoke = threadPool.invokeAll(callables, timeOut, timeUnit);
        List<T> ret = new ArrayList<>(invoke.size());
        for (Future<T> f : invoke) {
            ret.add(f.get());
        }
        return ret;
    }

    public ConcurrencyHelper<T> submitAllAndWaitIgnoringResults(Collection<Callable<T>> callables)
            throws InterruptedException
    {
        return submitAllAndWaitIgnoringResults(callables, 1, TimeUnit.DAYS);
    }

    public ConcurrencyHelper<T> submitAllAndWaitIgnoringResults(Collection<Callable<T>> callables,
            long timeOut,
            TimeUnit timeUnit)
            throws InterruptedException
    {
        threadPool.invokeAll(callables, timeOut, timeUnit);
        return this;
    }

    public List<T> submitAllAndWait(Collection<Callable<T>> callables)
            throws InterruptedException, ExecutionException
    {
        return submitAllAndWait(callables, 1, TimeUnit.DAYS);
    }

    public void shutdown()
    {
        threadPool.shutdown();
    }

    @Override
    public void close()
            throws InterruptedException, ExecutionException
    {
        threadPool.shutdown();
        threadPool.awaitTermination(1, TimeUnit.DAYS);
    }
}
