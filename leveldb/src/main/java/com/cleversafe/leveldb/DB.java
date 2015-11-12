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
import java.nio.file.Path;
import java.util.concurrent.CompletionStage;

public interface DB extends AsynchronousCloseable {
  CompletionStage<ByteBuffer> get(ByteBuffer key);

  CompletionStage<ByteBuffer> get(ByteBuffer key, ReadOptions options);

  CompletionStage<DBIterator> iterator(ReadOptions options);

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
   * Suspends any background compaction work. The future completes when the compactions have
   * suspended
   */
  CompletionStage<Void> suspendCompactions();

  /**
   * Resumes background compaction work after a suspension.
   */
  CompletionStage<Void> resumeCompactions();

  /**
   * Force a compaction of the specified key range.
   *
   * @param begin if null then compaction start from the first key
   * @param end if null then compaction ends at the last key
   */
  CompletionStage<Void> compactRange(ByteBuffer begin, ByteBuffer end);

  public interface DBFactory {
    CompletionStage<? extends DB> open(Path path, Options options);
  }
}
