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

import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.iq80.leveldb.DBBufferComparator;
import org.iq80.leveldb.util.ByteBuffers;
import org.iq80.leveldb.util.MemoryManagers;
import org.testng.annotations.Test;

public class BlockTest {
  private static final DBBufferComparator byteCompare = new BytewiseComparator();
  private static final Function<ByteBuffer, ByteBuffer> noopDecoder = Function.identity();

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testEmptyBuffer() throws Exception {
    new Block<ByteBuffer>(ByteBuffers.EMPTY_BUFFER, byteCompare, noopDecoder);
  }

  @Test
  public void testEmptyBlock() throws Exception {
    blockTest(Integer.MAX_VALUE);
  }

  @Test
  public void testSingleEntry() throws Exception {
    blockTest(Integer.MAX_VALUE, TestHelper.createBlockEntry("name", "dain sundstrom"));
  }

  @Test
  public void testMultipleEntriesWithNonSharedKey() throws Exception {
    blockTest(Integer.MAX_VALUE, TestHelper.createBlockEntry("beer", "Lagunitas IPA"),
        TestHelper.createBlockEntry("scotch", "Highland Park"));
  }

  @Test
  public void testMultipleEntriesWithSharedKey() throws Exception {
    blockTest(Integer.MAX_VALUE,
        TestHelper.createBlockEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’"),
        TestHelper.createBlockEntry("beer/ipa", "Lagunitas IPA"),
        TestHelper.createBlockEntry("scotch", "Highland Park"));
  }

  @Test
  public void testMultipleEntriesWithNonSharedKeyAndRestartPositions() throws Exception {
    final List<BlockEntry<ByteBuffer>> entries =
        asList(TestHelper.createBlockEntry("ale", "Lagunitas  Little Sumpin’ Sumpin’"),
            TestHelper.createBlockEntry("ipa", "Lagunitas IPA"),
            TestHelper.createBlockEntry("stout", "Lagunitas Imperial Stout"),
            TestHelper.createBlockEntry("strong", "Lagavulin"));

    for (int i = 1; i < entries.size(); i++) {
      blockTest(i, entries);
    }
  }

  @Test
  public void testMultipleEntriesWithSharedKeyAndRestartPositions() throws Exception {
    final List<BlockEntry<ByteBuffer>> entries =
        asList(TestHelper.createBlockEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’"),
            TestHelper.createBlockEntry("beer/ipa", "Lagunitas IPA"),
            TestHelper.createBlockEntry("beer/stout", "Lagunitas Imperial Stout"),
            TestHelper.createBlockEntry("scotch/light", "Oban 14"),
            TestHelper.createBlockEntry("scotch/medium", "Highland Park"),
            TestHelper.createBlockEntry("scotch/strong", "Lagavulin"));

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
    try (BlockBuilder builder =
        new BlockBuilder(256, blockRestartInterval, byteCompare, MemoryManagers.heap())) {
      for (final BlockEntry<ByteBuffer> entry : entries) {
        builder.add(entry.getKey(), entry.getValue());
      }

      assertEquals(builder.currentSizeEstimate(),
          TestHelper.estimateBlockSize(blockRestartInterval, entries));

      blockByteBuffer = builder.finish();

      assertEquals(builder.currentSizeEstimate(),
          TestHelper.estimateBlockSize(blockRestartInterval, entries));
    }

    final Block<ByteBuffer> block =
        new Block<ByteBuffer>(blockByteBuffer, byteCompare, noopDecoder);
    assertEquals(block.size(), TestHelper.estimateBlockSize(blockRestartInterval, entries));

    final BlockIterator<ByteBuffer> blockIterator = block.iterator();
    TestHelper.assertReverseSequence(blockIterator,
        Collections.<BlockEntry<ByteBuffer>>emptyList());
    TestHelper.assertSequence(blockIterator, entries);
    TestHelper.assertReverseSequence(blockIterator, reverseEntries);

    blockIterator.seekToFirst();
    TestHelper.assertReverseSequence(blockIterator,
        Collections.<BlockEntry<ByteBuffer>>emptyList());
    TestHelper.assertSequence(blockIterator, entries);
    TestHelper.assertReverseSequence(blockIterator, reverseEntries);

    blockIterator.seekToLast();
    if (reverseEntries.size() > 0) {
      TestHelper.assertSequence(blockIterator, reverseEntries.get(0));
      blockIterator.seekToLast();
      TestHelper.assertReverseSequence(blockIterator,
          reverseEntries.subList(1, reverseEntries.size()));
    }
    TestHelper.assertSequence(blockIterator, entries);

    blockIterator.seekToEnd();
    TestHelper.assertReverseSequence(blockIterator, reverseEntries);

    for (final BlockEntry<ByteBuffer> entry : entries) {
      final List<BlockEntry<ByteBuffer>> nextEntries =
          entries.subList(entries.indexOf(entry), entries.size());
      final List<BlockEntry<ByteBuffer>> prevEntries =
          reverseEntries.subList(reverseEntries.indexOf(entry), reverseEntries.size());
      blockIterator.seek(entry.getKey());
      TestHelper.assertSequence(blockIterator, nextEntries);

      blockIterator.seek(TestHelper.before(entry));
      TestHelper.assertSequence(blockIterator, nextEntries);

      blockIterator.seek(TestHelper.after(entry));
      TestHelper.assertSequence(blockIterator, nextEntries.subList(1, nextEntries.size()));

      blockIterator.seek(TestHelper.before(entry));
      TestHelper.assertReverseSequence(blockIterator, prevEntries.subList(1, prevEntries.size()));

      blockIterator.seek(entry.getKey());
      TestHelper.assertReverseSequence(blockIterator, prevEntries.subList(1, prevEntries.size()));

      blockIterator.seek(TestHelper.after(entry));
      TestHelper.assertReverseSequence(blockIterator, prevEntries);
    }

    blockIterator
        .seek(ByteBuffer.wrap(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}));
    TestHelper.assertSequence(blockIterator, Collections.<BlockEntry<ByteBuffer>>emptyList());
    TestHelper.assertReverseSequence(blockIterator, reverseEntries);
  }
}
