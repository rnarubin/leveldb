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

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import org.iq80.leveldb.Options;
import org.iq80.leveldb.util.Closeables;
import org.iq80.leveldb.table.FileChannelTable;
import org.iq80.leveldb.table.MMapTable;
import org.iq80.leveldb.table.Table;
import org.iq80.leveldb.table.TableIterator;
import org.iq80.leveldb.util.Finalizer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutionException;

public class TableCache
{
    private final LoadingCache<Long, TableAndFile> cache;
    private final Finalizer<Table> finalizer = new Finalizer<>(1);

    public TableCache(final File databaseDir,
            int tableCacheSize,
            final InternalKeyComparator userComparator,
            final Options options,
            final UncaughtExceptionHandler backgroundExceptionHandler)
    {
        Preconditions.checkNotNull(databaseDir, "databaseName is null");

        cache = CacheBuilder.newBuilder()
                .maximumSize(tableCacheSize)
                .removalListener(new RemovalListener<Long, TableAndFile>()
                {
                    @Override
                    public void onRemoval(RemovalNotification<Long, TableAndFile> notification)
                    {
                        Table table = notification.getValue().getTable();
                        finalizer.addCleanup(table, table.closer());
                        try {
                            table.release(); // corresponding to constructor implicit retain
                        }
                        catch (Exception e) {
                            backgroundExceptionHandler.uncaughtException(Thread.currentThread(), e);
                        }
                    }
                })
                .build(new CacheLoader<Long, TableAndFile>()
                {
                    @Override
                    public TableAndFile load(Long fileNumber)
                            throws IOException
                    {
                        return new TableAndFile(databaseDir, fileNumber, userComparator, options);
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
            while ((table = cache.get(number).getTable().retain()) == null)
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

    public void close()
    {
        cache.invalidateAll();
        finalizer.destroy();
    }

    public void evict(long number)
    {
        cache.invalidate(number);
    }

    private static final class TableAndFile
    {
        private final Table table;
        private FileChannel fileChannel;

        private TableAndFile(File databaseDir, long fileNumber, InternalKeyComparator userComparator, Options options)
                throws IOException
        {
            String tableFileName = Filename.tableFileName(fileNumber);
            File tableFile = new File(databaseDir, tableFileName);

            try {
                fileChannel = new FileInputStream(tableFile).getChannel();
            }
            catch (FileNotFoundException ldbNotFound) {
                try {
                    // attempt to open older .sst extension
                    tableFileName = Filename.sstTableFileName(fileNumber);
                    tableFile = new File(databaseDir, tableFileName);
                    fileChannel = new FileInputStream(tableFile).getChannel();
                }
                catch (FileNotFoundException sstNotFound) {
                    throw ldbNotFound;
                }
            }

            try {
                switch (options.ioImplemenation()) {
                    case MMAP:
                        table = new MMapTable(tableFile.getAbsolutePath(), fileChannel, userComparator, options);
                        break;
                    case FILE:
                        table = new FileChannelTable(tableFile.getAbsolutePath(), fileChannel, userComparator, options);
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown IO implementation:" + options.ioImplemenation());
                }
            }
            catch (IOException e) {
                Closeables.closeQuietly(fileChannel);
                throw e;
            }
        }

        public Table getTable()
        {
            return table;
        }
    }
}
