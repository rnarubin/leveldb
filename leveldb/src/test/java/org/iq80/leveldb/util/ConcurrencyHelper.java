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

public class ConcurrencyHelper<T>
        implements AutoCloseable
{
    ExecutorService threadPool;
    List<Future<T>> pendingWork;

    public ConcurrencyHelper()
    {
        this(Runtime.getRuntime().availableProcessors());
    }

    public ConcurrencyHelper(int threads)
    {
        threadPool = Executors.newFixedThreadPool(threads);
        pendingWork = new ArrayList<>();
    }

    public ConcurrencyHelper<T> submitAll(Collection<Callable<T>> callables)
    {
        for (Callable<T> c : callables) {
            pendingWork.add(threadPool.submit(c));
        }
        return this;
    }

    public List<T> waitForFinish()
            throws InterruptedException, ExecutionException
    {
        List<T> ret = new ArrayList<>(pendingWork.size());
        for (Future<T> f : pendingWork) {
            ret.add(f.get());
        }
        pendingWork.clear();
        return ret;
    }

    @Override
    public void close()
            throws InterruptedException, ExecutionException
    {
        threadPool.shutdown();
        threadPool.awaitTermination(1, TimeUnit.DAYS);
    }
}
