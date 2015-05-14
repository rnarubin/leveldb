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
    List<Future<T>> pendingWork;

    public ConcurrencyHelper(int threadCount)
    {
        this(threadCount, "unnamed-concurrency-helper");
    }

    public ConcurrencyHelper(int threads, String threadPrefix)
    {
        threadPool = Executors.newFixedThreadPool(threads,
                new ThreadFactoryBuilder().setNameFormat(threadPrefix + "-%d").build());
        pendingWork = new ArrayList<>();
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
