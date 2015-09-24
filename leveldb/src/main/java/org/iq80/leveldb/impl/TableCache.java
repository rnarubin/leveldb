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
package org.iq80.leveldb.impl;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.CompletionStage;

import org.iq80.leveldb.Env.DBHandle;
import org.iq80.leveldb.FileInfo;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.table.Table;
import org.iq80.leveldb.table.TableIterator;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public final class TableCache implements AutoCloseable {
  private final LoadingCache<Long, CompletionStage<Table>> cache;

  public TableCache(final DBHandle dbHandle, final int tableCacheSize,
      final InternalKeyComparator userComparator, final Options options,
      final UncaughtExceptionHandler backgroundExceptionHandler) {
    this.cache = CacheBuilder.newBuilder().maximumSize(tableCacheSize)
        .<Long, CompletionStage<Table>>removalListener(
            notification -> notification.getValue().whenComplete((table, openException) -> {
              if (openException != null) {
                // table was opened successfully
                table.release().whenComplete((voided, closeException) -> {
                  if (closeException != null) {
                    backgroundExceptionHandler.uncaughtException(Thread.currentThread(),
                        closeException);
                  }
                });
              }
            }))
        .build(new CacheLoader<Long, CompletionStage<Table>>() {
          @Override
          public CompletionStage<Table> load(final Long fileNumber) {
            return options.env().openRandomReadFile(FileInfo.table(dbHandle, fileNumber))
                .thenCompose(file -> Table.newTable(file, userComparator, options));
          }
        });
  }

  public CompletionStage<TableIterator> newIterator(final FileMetaData file) {
    return newIterator(file.getNumber());
  }

  public CompletionStage<TableIterator> newIterator(final long number) {
    return getTable(number).thenApply(Table::iterator);
  }

  public CompletionStage<Long> getApproximateOffsetOf(final FileMetaData file,
      final InternalKey key) {
    return getTable(file.getNumber()).thenCompose(table -> {
      final long offset = table.getApproximateOffsetOf(key);
      return table.release().thenApply(voided -> offset);
    });
  }

  private CompletionStage<Table> getTable(final long number) {
    final CompletionStage<Table> attempt = cache.getUnchecked(number);
    return attempt.thenCompose(
        // recurse in the event of a race between this call's retain and an
        // eviction-refcount-decremented dispose
        table -> table.retain() != null ? attempt : getTable(number));
  }

  @Override
  public void close() {
    cache.invalidateAll();
  }

  public void evict(final long number) {
    cache.invalidate(number);
  }
}
