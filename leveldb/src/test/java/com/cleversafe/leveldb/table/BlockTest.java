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

import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.testng.annotations.Test;

import com.cleversafe.leveldb.table.Block;
import com.cleversafe.leveldb.table.BlockBuilder;
import com.cleversafe.leveldb.table.BlockEntry;
import com.cleversafe.leveldb.table.BlockIterator;
import com.cleversafe.leveldb.util.ByteBuffers;
import com.cleversafe.leveldb.util.MemoryManagers;

public class BlockTest {
  private static final Function<ByteBuffer, ByteBuffer> noopDecoder = Function.identity();

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testEmptyBuffer() throws Exception {
    new Block<ByteBuffer>(ByteBuffers.EMPTY_BUFFER, TestUtils.byteComparator, noopDecoder);
  }

  @Test
  public void testEmptyBlock() throws Exception {
    blockTest(Integer.MAX_VALUE);
  }

  @Test
  public void testSingleEntry() throws Exception {
    blockTest(Integer.MAX_VALUE, TestUtils.createBlockEntry("name", "dain sundstrom"));
  }

  @Test
  public void testMultipleEntriesWithNonSharedKey() throws Exception {
    blockTest(Integer.MAX_VALUE, TestUtils.createBlockEntry("beer", "Lagunitas IPA"),
        TestUtils.createBlockEntry("scotch", "Highland Park"));
  }

  @Test
  public void testMultipleEntriesWithSharedKey() throws Exception {
    blockTest(Integer.MAX_VALUE,
        TestUtils.createBlockEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’"),
        TestUtils.createBlockEntry("beer/ipa", "Lagunitas IPA"),
        TestUtils.createBlockEntry("scotch", "Highland Park"));
  }

  @Test
  public void testMultipleEntriesWithNonSharedKeyAndRestartPositions() throws Exception {
    final List<BlockEntry<ByteBuffer>> entries =
        asList(TestUtils.createBlockEntry("ale", "Lagunitas  Little Sumpin’ Sumpin’"),
            TestUtils.createBlockEntry("ipa", "Lagunitas IPA"),
            TestUtils.createBlockEntry("stout", "Lagunitas Imperial Stout"),
            TestUtils.createBlockEntry("strong", "Lagavulin"));

    for (int i = 1; i < entries.size(); i++) {
      blockTest(i, entries);
    }
  }

  @Test
  public void testMultipleEntriesWithSharedKeyAndRestartPositions() throws Exception {
    final List<BlockEntry<ByteBuffer>> entries =
        asList(TestUtils.createBlockEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’"),
            TestUtils.createBlockEntry("beer/ipa", "Lagunitas IPA"),
            TestUtils.createBlockEntry("beer/stout", "Lagunitas Imperial Stout"),
            TestUtils.createBlockEntry("scotch/light", "Oban 14"),
            TestUtils.createBlockEntry("scotch/medium", "Highland Park"),
            TestUtils.createBlockEntry("scotch/strong", "Lagavulin"));

    for (int i = 1; i < entries.size(); i++) {
      blockTest(i, entries);
    }
  }

  @SafeVarargs
  private static void blockTest(final int blockRestartInterval,
      final BlockEntry<ByteBuffer>... entries) {
    blockTest(blockRestartInterval, asList(entries));
  }

  private static void blockTest(final int blockRestartInterval,
      final List<BlockEntry<ByteBuffer>> entries) {
    final List<BlockEntry<ByteBuffer>> reverseEntries = new ArrayList<>(entries);
    Collections.reverse(reverseEntries);

    ByteBuffer blockByteBuffer;
    try (BlockBuilder builder = new BlockBuilder(256, blockRestartInterval,
        TestUtils.byteComparator, MemoryManagers.heap())) {
      for (final BlockEntry<ByteBuffer> entry : entries) {
        builder.add(entry.getKey(), entry.getValue());
      }

      assertEquals(builder.currentSizeEstimate(),
          TestUtils.estimateBlockSize(blockRestartInterval, entries));

      blockByteBuffer = builder.finish();

      assertEquals(builder.currentSizeEstimate(),
          TestUtils.estimateBlockSize(blockRestartInterval, entries));
    }

    final Block<ByteBuffer> block =
        new Block<ByteBuffer>(blockByteBuffer, TestUtils.byteComparator, noopDecoder);
    assertEquals(block.size(), TestUtils.estimateBlockSize(blockRestartInterval, entries));

    final BlockIterator<ByteBuffer> blockIterator = block.iterator();
    TestUtils.assertReverseSequence(blockIterator, Collections.<BlockEntry<ByteBuffer>>emptyList());
    TestUtils.assertSequence(blockIterator, entries);
    TestUtils.assertReverseSequence(blockIterator, reverseEntries);

    blockIterator.seekToFirst();
    TestUtils.assertReverseSequence(blockIterator, Collections.<BlockEntry<ByteBuffer>>emptyList());
    TestUtils.assertSequence(blockIterator, entries);
    TestUtils.assertReverseSequence(blockIterator, reverseEntries);

    blockIterator.seekToLast();
    if (reverseEntries.size() > 0) {
      TestUtils.assertSequence(blockIterator, reverseEntries.get(0));
      blockIterator.seekToLast();
      TestUtils.assertReverseSequence(blockIterator,
          reverseEntries.subList(1, reverseEntries.size()));
    }
    TestUtils.assertSequence(blockIterator, entries);

    blockIterator.seekToEnd();
    TestUtils.assertReverseSequence(blockIterator, reverseEntries);

    for (final BlockEntry<ByteBuffer> entry : entries) {
      final List<BlockEntry<ByteBuffer>> nextEntries =
          entries.subList(entries.indexOf(entry), entries.size());
      final List<BlockEntry<ByteBuffer>> prevEntries =
          reverseEntries.subList(reverseEntries.indexOf(entry), reverseEntries.size());
      blockIterator.seek(entry.getKey());
      TestUtils.assertSequence(blockIterator, nextEntries);

      blockIterator.seek(TestUtils.before(entry.getKey()));
      TestUtils.assertSequence(blockIterator, nextEntries);

      blockIterator.seek(TestUtils.after(entry.getKey()));
      TestUtils.assertSequence(blockIterator, nextEntries.subList(1, nextEntries.size()));

      blockIterator.seek(TestUtils.before(entry.getKey()));
      TestUtils.assertReverseSequence(blockIterator, prevEntries.subList(1, prevEntries.size()));

      blockIterator.seek(entry.getKey());
      TestUtils.assertReverseSequence(blockIterator, prevEntries.subList(1, prevEntries.size()));

      blockIterator.seek(TestUtils.after(entry.getKey()));
      TestUtils.assertReverseSequence(blockIterator, prevEntries);
    }

    blockIterator
        .seek(ByteBuffer.wrap(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}));
    TestUtils.assertSequence(blockIterator, Collections.<BlockEntry<ByteBuffer>>emptyList());
    TestUtils.assertReverseSequence(blockIterator, reverseEntries);
  }
}
