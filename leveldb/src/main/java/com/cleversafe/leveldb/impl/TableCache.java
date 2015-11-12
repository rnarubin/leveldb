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
package com.cleversafe.leveldb.impl;

import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentSkipListSet;

import com.cleversafe.leveldb.AsynchronousCloseable;
import com.cleversafe.leveldb.Compression;
import com.cleversafe.leveldb.Env;
import com.cleversafe.leveldb.Env.DBHandle;
import com.cleversafe.leveldb.FileInfo;
import com.cleversafe.leveldb.table.Table;
import com.cleversafe.leveldb.table.Table.TableIterator;
import com.cleversafe.leveldb.util.CompletableFutures;
import com.cleversafe.leveldb.util.SeekingAsynchronousIterator;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class TableCache implements AsynchronousCloseable {
  /**
   * a set that retains tables removed from the cache before they are closed. tables are added upon
   * cache invalidation, and removed upon successful release -- if they encounter an exception on
   * release (i.e. IOException on dispose) they remain in the set to throw an exception on the
   * TableCache's close
   */
  private final Set<CompletionStage<Void>> pendingRemovals = new ConcurrentSkipListSet<>();
  // TODO look into ben-manes/caffeine
  private final LoadingCache<Long, CompletionStage<Table>> cache;

  public TableCache(final DBHandle dbHandle, final int tableCacheSize,
      final Comparator<InternalKey> internalKeyComparator, final Env env,
      final boolean verifyChecksums, final Compression compression,
      // TODO pending removals might obviate bgExceptionHandler
      final UncaughtExceptionHandler backgroundExceptionHandler) {
    this.cache = CacheBuilder.newBuilder().maximumSize(tableCacheSize)
        .<Long, CompletionStage<Table>>removalListener(notification -> {
          notification.getValue().whenComplete((table, openException) -> {
            if (openException == null) {
              // table was originally opened successfully
              final CompletionStage<Void> release = table.releaseNullable();
              if (release != null) {
                pendingRemovals.add(release);
                release.whenComplete((voided, closeException) -> {
                  if (closeException != null) {
                    backgroundExceptionHandler.uncaughtException(Thread.currentThread(),
                        closeException);
                  } else {
                    pendingRemovals.remove(release);
                  }
                });
              }
            }
          });
        }).build(new CacheLoader<Long, CompletionStage<Table>>() {
          @Override
          public CompletionStage<Table> load(final Long fileNumber) {
            return loadTable(env, dbHandle, fileNumber, internalKeyComparator, verifyChecksums,
                compression);
          }
        });
  }

  public CompletionStage<? extends SeekingAsynchronousIterator<InternalKey, ByteBuffer>> tableIterator(
      final FileMetaData file) {
    return tableIterator(file.getNumber());
  }

  public CompletionStage<TableIterator> tableIterator(final long number) {
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

  public void evict(final long number) {
    cache.invalidate(number);
  }

  @Override
  public CompletionStage<Void> asyncClose() {
    cache.invalidateAll();
    return CompletableFutures.allOfVoid(pendingRemovals.stream());
  }

  public static CompletionStage<Table> loadTable(final Env env, final DBHandle dbHandle,
      final long fileNumber, final Comparator<InternalKey> internalKeyComparator,
      final boolean verifyChecksums, final Compression compression) {
    return env.openRandomReadFile(FileInfo.table(dbHandle, fileNumber))
        .thenCompose(file -> CompletableFutures.composeOnException(
            Table.newTable(file, internalKeyComparator, verifyChecksums, compression),
            throwable -> file.asyncClose()));
  }
}
