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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
    private final AtomicInteger submittedTasks;

    public AsyncWriter()
    {
        this.workerPool = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("leveldb-async-writer-%s").build());
        this.isClosed = new AtomicBoolean(false);
        this.submittedTasks = new AtomicInteger(0);
    }

    public Future<Long> submit(final List<ByteBuffer[]> listOfBuffers)
    {
        Preconditions.checkState(!isClosed.get(), "Cannot submit to closed writer");

        //if submissions outpace writing, throttling the submitters is necessary
        //to allow the writer to catch up
        throttleIfNecessary(submittedTasks.incrementAndGet());

        Future<Long> future = workerPool.submit(new Callable<Long>()
        {
            @Override
            public Long call()
                    throws IOException
            {
                long written = 0;
                for (ByteBuffer[] b : listOfBuffers) {
                    written += write(b);
                }
                submittedTasks.decrementAndGet();
                return written;
            }
        });

        return future;
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
        workerPool.shutdown();
        try {
            workerPool.awaitTermination(1, TimeUnit.HOURS);
        }
        catch (InterruptedException e) {
            throw new IOException("Interrupted while closing async writer", e);
        }
    }

    protected void throttleIfNecessary(int workPending)
    {

    }
}


