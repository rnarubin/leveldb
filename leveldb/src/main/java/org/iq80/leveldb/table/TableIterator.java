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
package org.iq80.leveldb.table;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;

import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.ReverseSeekingIterator;
import org.iq80.leveldb.table.TableIterator.WrappedBlockIterator;
import org.iq80.leveldb.util.Iterators.AsyncWrappedSeekingIterator;
import org.iq80.leveldb.util.TwoStageIterator;

public final class TableIterator extends
    TwoStageIterator<BlockIterator<InternalKey>, WrappedBlockIterator<InternalKey>, ByteBuffer> {
  private final Table table;

  public TableIterator(final Table table, final BlockIterator<InternalKey> indexIterator) {
    super(indexIterator);
    this.table = table;
  }

  @Override
  protected CompletionStage<WrappedBlockIterator<InternalKey>> getData(
      final ByteBuffer blockHandle) {
    return table.openBlock(blockHandle)
        .thenApply(block -> new WrappedBlockIterator<>(block.iterator()));
  }

  static class WrappedBlockIterator<T> extends AsyncWrappedSeekingIterator<T, ByteBuffer> {
    public WrappedBlockIterator(final ReverseSeekingIterator<T, ByteBuffer> iter) {
      super(iter);
    }
  }

  @Override
  public CompletionStage<Void> asyncClose() {
    return table.release();
  }
}
