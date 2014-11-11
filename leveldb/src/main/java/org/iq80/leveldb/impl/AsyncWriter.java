/**
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

package org.iq80.leveldb.impl;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.iq80.leveldb.ThrottlePolicies;
import org.iq80.leveldb.ThrottlePolicy;
import org.iq80.leveldb.impl.DbImpl.BackgroundExceptionHandler;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public abstract class AsyncWriter
        implements Closeable
{
    /*
     * a class used to delegate writes to the log file to a dedicated thread
     */
    private final ExecutorService workerPool;
    private final AtomicBoolean isClosed;
    private final BackgroundExceptionHandler bgExceptionHandler;
    private final ThrottlePolicy throttlePolicy;
    private static final int MAX_PERMITS = Integer.MAX_VALUE;
    private final Semaphore submissions;

    public AsyncWriter(BackgroundExceptionHandler bgExceptionHandler, ThrottlePolicy throttlePolicy)
    {
        this.workerPool = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("leveldb-async-writer-%s").build());
        this.isClosed = new AtomicBoolean(false);
        this.bgExceptionHandler = bgExceptionHandler;
        this.throttlePolicy = throttlePolicy != null ? throttlePolicy : ThrottlePolicies.noThrottle();

        // submitted tasks will acquire one from the lock;
        // closing may only occur when all permits are available (all submissions completed)
        this.submissions = new Semaphore(MAX_PERMITS);
    }

    public Future<Long> submit(final List<ByteBuffer[]> listOfBuffers)
    {
        Preconditions.checkState(!isClosed.get() && submissions.tryAcquire(), "Cannot submit to closed writer");

        //if submissions outpace flushing, throttling the submitters is necessary
        //to allow the flusher to catch up
        try{
           throttlePolicy.throttleIfNecessary(MAX_PERMITS - submissions.availablePermits());
        }
        catch(Throwable t){
           submissions.release();
           throw t;
        }

        return workerPool.submit(new Callable<Long>()
        {
            @Override
            public Long call()
                    throws IOException
            {
                long written = 0;
                try{
                   for (ByteBuffer[] b : listOfBuffers) {
                       written += write(b);
                   }
                }
                catch(Throwable t){
                   if(bgExceptionHandler == null){
                      //if no handler is provided, rethrow to be caught in callable's get()
                      throw t;
                   }
                   bgExceptionHandler.handle(t);
                }
                finally{
                   submissions.release();
                   throttlePolicy.notifyCompletion();
                }
                return written;
            }
        });
    }

    protected abstract long write(ByteBuffer[] buffers)
            throws IOException;

    @Override
    public void close()
            throws IOException
    {
        if (isClosed.getAndSet(true)) {
            return;
        }
        try {
            //new tasks can no longer be submitted
            //we must wait for pending tasks to complete (both those already
            //in the thread pool, and those currently throttled) before closing
            submissions.acquire(MAX_PERMITS);

            workerPool.shutdown();
            workerPool.awaitTermination(10, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}