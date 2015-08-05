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
package org.iq80.leveldb.impl;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import org.iq80.leveldb.Env.DBHandle;
import org.iq80.leveldb.FileInfo;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.table.Table;
import org.iq80.leveldb.table.TableIterator;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ExecutionException;

public final class TableCache
        implements AutoCloseable
{
    private final LoadingCache<Long, Table> cache;

    public TableCache(final DBHandle dbHandle,
            int tableCacheSize,
            final InternalKeyComparator userComparator,
            final Options options,
            final UncaughtExceptionHandler backgroundExceptionHandler)
    {
        cache = CacheBuilder.newBuilder()
                .maximumSize(tableCacheSize)
                .removalListener(new RemovalListener<Long, Table>()
                {
                    @Override
                    public void onRemoval(RemovalNotification<Long, Table> notification)
                    {
                        Table table = notification.getValue();
                        try {
                            table.release(); // corresponding to constructor implicit retain
                        }
                        catch (Exception e) {
                            backgroundExceptionHandler.uncaughtException(Thread.currentThread(), e);
                        }
                    }
                })
                .build(new CacheLoader<Long, Table>()
                {
                    @Override
                    public Table load(Long fileNumber)
                            throws IOException
                    {
                        return openTableFile(dbHandle, fileNumber, userComparator, options);
                    }
                });
    }

    public TableIterator newIterator(FileMetaData file)
    {
        return newIterator(file.getNumber());
    }

    public TableIterator newIterator(long number)
    {
        return getTable(number).iterator();
    }

    public long getApproximateOffsetOf(FileMetaData file, InternalKey key)
    {
        try (Table table = getTable(file.getNumber())) {
            return table.getApproximateOffsetOf(key);
        }
    }

    private Table getTable(long number)
    {
        Table table;
        try {
            // minuscule chance of race between cache eviction and table release.
            // re-read cache until we get a winner
            while ((table = cache.get(number).retain()) == null)
                ;
        }
        catch (ExecutionException e) {
            Throwable cause = e;
            if (e.getCause() != null) {
                cause = e.getCause();
            }
            throw new RuntimeException("Could not open table " + number, cause);
        }
        return table;
    }

    @Override
    public void close()
    {
        cache.invalidateAll();
    }

    public void evict(long number)
    {
        cache.invalidate(number);
    }

    private static Table openTableFile(DBHandle dbHandle,
            long fileNumber,
            InternalKeyComparator userComparator,
            Options options)
            throws IOException
    {
        return new Table(options.env().openRandomReadFile(FileInfo.table(dbHandle, fileNumber)), userComparator,
                options);
    }
}
