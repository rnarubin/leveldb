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
package com.cleversafe.leveldb;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

import com.cleversafe.leveldb.Env.DBHandle;
import com.cleversafe.leveldb.util.SeekingAsynchronousIterator;

public interface DB extends AsynchronousCloseable {
  CompletionStage<Optional<ByteBuffer>> get(ByteBuffer key);

  CompletionStage<Optional<ByteBuffer>> get(ByteBuffer key, ReadOptions options);

  CompletionStage<SeekingAsynchronousIterator<ByteBuffer, ByteBuffer>> iterator(
      ReadOptions options);

  CompletionStage<?> put(ByteBuffer key, ByteBuffer value);

  CompletionStage<?> delete(ByteBuffer key);

  CompletionStage<?> write(WriteBatch updates);

  WriteBatch createWriteBatch();

  /**
   * @return null if options.isSnapshot()==false otherwise returns a snapshot of the DB after this
   *         operation.
   */
  CompletionStage<Snapshot> put(ByteBuffer key, ByteBuffer value, WriteOptions options);

  /**
   * @return null if options.isSnapshot()==false otherwise returns a snapshot of the DB after this
   *         operation.
   */
  CompletionStage<Snapshot> delete(ByteBuffer key, WriteOptions options);

  /**
   * @return null if options.isSnapshot()==false otherwise returns a snapshot of the DB after this
   *         operation.
   */
  CompletionStage<Snapshot> write(WriteBatch updates, WriteOptions options);

  Snapshot getSnapshot();

  CompletionStage<Long> getApproximateSize(ByteBuffer begin, ByteBuffer end);

  // TODO get these
  String getProperty(String name);

  /**
   * Suspends any background compaction work and prevents any new compactions from beginning. The
   * returned stage completes when all running compactions have completed.
   * <p>
   * Once suspended, and after any pending write operations have completed and are no longer
   * submitted by the application, the DB will be in a 'frozen' state. No underlying files will be
   * changed during this time. This may be suitable for performing back-ups or live introspection
   * <p>
   * Only one suspension may be active at any time. Attempting to suspend compactions while one is
   * active will throw an IllegalStateException
   *
   * @throws IllegalStateException if a suspension is already active
   */
  CompletionStage<CompactionSuspension> suspendCompactions() throws IllegalStateException;

  /**
   * An object representing suspended background compactions. Background processing may be resumed
   * by calling {@link #resume()}
   */
  public interface CompactionSuspension {
    /**
     * Allows the DB to resume any compactions that were underway at the time of suspension, and to
     * schedule new compactions as necessary
     */
    void resume();
  }

  /**
   * Force a compaction of the specified key range.
   *
   * @param begin if null then compaction starts from the first key
   * @param end if null then compaction ends at the last key
   */
  CompletionStage<Void> compactRange(ByteBuffer begin, ByteBuffer end);

  public interface DBFactory {
    CompletionStage<? extends DB> open(DBHandle handle, Options options);
  }
}
