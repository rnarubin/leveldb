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

package com.cleversafe.leveldb.table;

import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.cleversafe.leveldb.FileInfo;
import com.cleversafe.leveldb.Env.SequentialWriteFile;
import com.cleversafe.leveldb.impl.InternalKey;
import com.cleversafe.leveldb.impl.TransientInternalKey;
import com.cleversafe.leveldb.impl.ValueType;
import com.cleversafe.leveldb.table.TableBuilder;
import com.cleversafe.leveldb.util.EnvDependentTest;
import com.cleversafe.leveldb.util.FileEnvTestProvider;
import com.cleversafe.leveldb.util.SizeOf;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;

public abstract class TableBuilderTest extends EnvDependentTest {

  @Test
  public void testBuild() throws Exception {
    final Queue<Entry<InternalKey, ByteBuffer>> entries = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < 256; i++) {
      final ByteBuffer data = ByteBuffer.allocate(100).put(0, (byte) i);
      entries.add(Maps.immutableEntry(new TransientInternalKey(data, i, ValueType.VALUE), data));
    }
    Assert.assertTrue(Ordering.from(TestUtils.keyComparator)
        .isOrdered(entries.stream().map(entry -> entry.getKey()).collect(Collectors.toList())));

    final SequentialWriteFile file = getEnv()
        .openSequentialWriteFile(FileInfo.table(getHandle(), 0)).toCompletableFuture().get();

    final int blockRestartInterval = 16;
    final int blockSize = 4096;
    try (
        final TableBuilder builder =
            new TableBuilder(blockRestartInterval, blockSize, null, file, TestUtils.keyComparator);
        AutoCloseable c = TestUtils.autoCloseable(file)) {

      Assert.assertEquals(builder.getFileSizeEstimate(), SizeOf.SIZE_OF_INT);

      final Entry<InternalKey, ByteBuffer> firstEntry = entries.poll();
      CompletionStage<Void> chain = builder.add(firstEntry.getKey(), firstEntry.getValue());
      chain.toCompletableFuture().get();
      final int firstSize = 200 // key + value
          + SizeOf.SIZE_OF_BYTE * 3 // varint shared, nonshared, value
          + SizeOf.SIZE_OF_LONG // seq + type
          + SizeOf.SIZE_OF_INT // 1 restart position
          + SizeOf.SIZE_OF_INT; // restart count

      Assert.assertEquals(builder.getFileSizeEstimate(), firstSize);
      for (final Entry<InternalKey, ByteBuffer> entry : entries) {
        chain = chain.thenCompose(voided -> builder.add(entry.getKey(), entry.getValue()));
      }
      chain.toCompletableFuture().get();
      Assert.assertEquals(builder.getFileSizeEstimate(), 54228);
      final long fileSize = builder.finish().toCompletableFuture().get();
      Assert.assertEquals(fileSize, 55756);
    }
  }

  public static class FileTableBuilderTest extends TableBuilderTest implements FileEnvTestProvider {
  }

}
