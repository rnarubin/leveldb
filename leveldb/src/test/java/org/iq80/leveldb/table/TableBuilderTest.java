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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.iq80.leveldb.Env.SequentialWriteFile;
import org.iq80.leveldb.FileInfo;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.InternalKeyComparator;
import org.iq80.leveldb.impl.TransientInternalKey;
import org.iq80.leveldb.impl.ValueType;
import org.iq80.leveldb.table.TableBuilder.BuilderState;
import org.iq80.leveldb.util.EnvDependentTest;
import org.iq80.leveldb.util.FileEnvTestProvider;
import org.iq80.leveldb.util.SizeOf;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;

public abstract class TableBuilderTest extends EnvDependentTest {

  @Test
  public void testBuild() throws InterruptedException, ExecutionException, IOException {
    final InternalKeyComparator comparator = new InternalKeyComparator(new BytewiseComparator());
    final Queue<Entry<InternalKey, ByteBuffer>> entries = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < 256; i++) {
      final ByteBuffer data = ByteBuffer.allocate(100).put(0, (byte) i);
      entries.add(Maps.immutableEntry(new TransientInternalKey(data, i, ValueType.VALUE), data));
    }
    Assert.assertTrue(comparator
        .isOrdered(entries.stream().map(entry -> entry.getKey()).collect(Collectors.toList())));

    final SequentialWriteFile file = getEnv()
        .openSequentialWriteFile(FileInfo.table(getHandle(), 0)).toCompletableFuture().get();

    final int blockRestartInterval = 16;
    final int blockSize = 4096;
    try (final TableBuilder builder = new TableBuilder(Options.make().compression(null)
        .blockRestartInterval(blockRestartInterval).blockSize(blockSize), file, comparator)) {

      BuilderState initstate = builder.init();
      Assert.assertTrue(initstate.isEmpty());
      Assert.assertEquals(initstate.getFileSizeEstimate(), SizeOf.SIZE_OF_INT);

      final Entry<InternalKey, ByteBuffer> firstEntry = entries.poll();
      CompletionStage<BuilderState> chain =
          builder.add(firstEntry.getKey(), firstEntry.getValue(), initstate);
      initstate = chain.toCompletableFuture().get();
      Assert.assertFalse(initstate.isEmpty());
      final int firstSize = 200 // key + value
          + SizeOf.SIZE_OF_BYTE * 3 // varint shared, nonshared, value
          + SizeOf.SIZE_OF_LONG // seq + type
          + SizeOf.SIZE_OF_INT // 1 restart position
          + SizeOf.SIZE_OF_INT; // restart count

      Assert.assertEquals(initstate.getFileSizeEstimate(), firstSize);
      for (final Entry<InternalKey, ByteBuffer> entry : entries) {
        chain = chain.thenCompose(state -> builder.add(entry.getKey(), entry.getValue(), state));
      }
      final BuilderState lastState = chain.toCompletableFuture().get();
      Assert.assertEquals(lastState.getFileSizeEstimate(), 54228);
      final long fileSize = builder.finish(lastState).toCompletableFuture().get();
      Assert.assertEquals(fileSize, 55734);
    }
  }

  public static class FileTableBuilderTest extends TableBuilderTest implements FileEnvTestProvider {
  }

}
