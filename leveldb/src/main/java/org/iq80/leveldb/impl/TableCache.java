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

import org.iq80.leveldb.Compression;
import org.iq80.leveldb.Env;
import org.iq80.leveldb.Env.DBHandle;
import org.iq80.leveldb.FileInfo;
import org.iq80.leveldb.table.Table;
import org.iq80.leveldb.table.Table.TableIterator;
import org.iq80.leveldb.util.CompletableFutures;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class TableCache implements AutoCloseable {
  private final LoadingCache<Long, CompletionStage<Table>> cache;
  private final Env env;

  public TableCache(final DBHandle dbHandle, final int tableCacheSize,
      final InternalKeyComparator internalKeyComparator, final Env env,
      final boolean verifyChecksums, final Compression compression,
      final UncaughtExceptionHandler backgroundExceptionHandler) {
    this.env = env;
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
            return env.openRandomReadFile(FileInfo.table(dbHandle, fileNumber))
                .thenCompose(file -> {
              final CompletionStage<Table> table =
                  Table.newTable(file, internalKeyComparator, verifyChecksums, compression);
              return CompletableFutures.composeUnconditionally(table, optTable -> {
                if (optTable.isPresent()) {
                  return table;
                } else {
                  // error in initializing table
                  return file.asyncClose().thenApply(voided -> null);
                }
              });
            });
          }
        });
  }

  public CompletionStage<TableIterator> tableIterator(final FileMetaData file) {
    return tableIterator(file.getNumber());
  }

  public CompletionStage<TableIterator> tableIterator(final long number) {
    return getTable(number).thenApply(Table::iterator);
  }

  public CompletionStage<Long> getApproximateOffsetOf(final FileMetaData file,
      final InternalKey key) {
    return getTable(file.getNumber()).thenComposeAsync(table -> {
      final long offset = table.getApproximateOffsetOf(key);
      return table.release().thenApply(voided -> offset);
    } , env.getExecutor());
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
