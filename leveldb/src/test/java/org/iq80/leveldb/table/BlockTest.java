/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.iq80.leveldb.table;

import org.iq80.leveldb.DBBufferComparator;
import org.iq80.leveldb.util.ByteBuffers;
import org.iq80.leveldb.util.MemoryManagers;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;

public class BlockTest
{
    private static final DBBufferComparator byteCompare = new BytewiseComparator();
    private static final Decoder<ByteBuffer> noopDecoder = new Decoder<ByteBuffer>()
    {
        @Override
        public ByteBuffer decode(ByteBuffer b)
        {
            return b;
        }
    };

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testEmptyBuffer()
            throws Exception
    {
        new Block<ByteBuffer>(ByteBuffers.EMPTY_BUFFER, byteCompare, MemoryManagers.heap(), noopDecoder);
    }

    @Test
    public void testEmptyBlock()
            throws Exception
    {
        blockTest(Integer.MAX_VALUE);
    }

    @Test
    public void testSingleEntry()
            throws Exception
    {
        blockTest(Integer.MAX_VALUE,
                BlockHelper.createBlockEntry("name", "dain sundstrom"));
    }

    @Test
    public void testMultipleEntriesWithNonSharedKey()
            throws Exception
    {
        blockTest(Integer.MAX_VALUE,
                BlockHelper.createBlockEntry("beer", "Lagunitas IPA"),
                BlockHelper.createBlockEntry("scotch", "Highland Park"));
    }

    @Test
    public void testMultipleEntriesWithSharedKey()
            throws Exception
    {
        blockTest(Integer.MAX_VALUE,
                BlockHelper.createBlockEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’"),
                BlockHelper.createBlockEntry("beer/ipa", "Lagunitas IPA"),
                BlockHelper.createBlockEntry("scotch", "Highland Park"));
    }

    @Test
    public void testMultipleEntriesWithNonSharedKeyAndRestartPositions()
            throws Exception
    {
        List<BlockEntry<ByteBuffer>> entries = asList(
                BlockHelper.createBlockEntry("ale", "Lagunitas  Little Sumpin’ Sumpin’"),
                BlockHelper.createBlockEntry("ipa", "Lagunitas IPA"),
                BlockHelper.createBlockEntry("stout", "Lagunitas Imperial Stout"),
                BlockHelper.createBlockEntry("strong", "Lagavulin"));

        for (int i = 1; i < entries.size(); i++) {
            blockTest(i, entries);
        }
    }

    @Test
    public void testMultipleEntriesWithSharedKeyAndRestartPositions()
            throws Exception
    {
        List<BlockEntry<ByteBuffer>> entries = asList(
                BlockHelper.createBlockEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’"),
                BlockHelper.createBlockEntry("beer/ipa", "Lagunitas IPA"),
                BlockHelper.createBlockEntry("beer/stout", "Lagunitas Imperial Stout"),
                BlockHelper.createBlockEntry("scotch/light", "Oban 14"),
                BlockHelper.createBlockEntry("scotch/medium", "Highland Park"),
                BlockHelper.createBlockEntry("scotch/strong", "Lagavulin"));

        for (int i = 1; i < entries.size(); i++) {
            blockTest(i, entries);
        }
    }

    @SafeVarargs
    private static void blockTest(int blockRestartInterval, BlockEntry<ByteBuffer>... entries)
    {
        blockTest(blockRestartInterval, asList(entries));
    }

    private static void blockTest(int blockRestartInterval, List<BlockEntry<ByteBuffer>> entries)
    {
        List<BlockEntry<ByteBuffer>> reverseEntries = new ArrayList<>(entries);
        Collections.reverse(reverseEntries);

        BlockBuilder builder = new BlockBuilder(256, blockRestartInterval, byteCompare, MemoryManagers.heap());

        for (BlockEntry<ByteBuffer> entry : entries) {
            builder.add(entry.getKey(), entry.getValue());
        }

        assertEquals(builder.currentSizeEstimate(), BlockHelper.estimateBlockSize(blockRestartInterval, entries));
        ByteBuffer blockByteBuffer = builder.finish();
        assertEquals(builder.currentSizeEstimate(), BlockHelper.estimateBlockSize(blockRestartInterval, entries));

        Block<ByteBuffer> block = new Block<ByteBuffer>(blockByteBuffer, byteCompare, MemoryManagers.heap(),
                noopDecoder);
        assertEquals(block.size(), BlockHelper.estimateBlockSize(blockRestartInterval, entries));

        BlockIterator<ByteBuffer> blockIterator = block.iterator();
        BlockHelper.assertReverseSequence(blockIterator, Collections.<BlockEntry<ByteBuffer>> emptyList());
        BlockHelper.assertSequence(blockIterator, entries);
        BlockHelper.assertReverseSequence(blockIterator, reverseEntries);

        blockIterator.seekToFirst();
        BlockHelper.assertReverseSequence(blockIterator, Collections.<BlockEntry<ByteBuffer>> emptyList());
        BlockHelper.assertSequence(blockIterator, entries);
        BlockHelper.assertReverseSequence(blockIterator, reverseEntries);

        blockIterator.seekToLast();
        if (reverseEntries.size() > 0) {
            BlockHelper.assertSequence(blockIterator, reverseEntries.get(0));
            blockIterator.seekToLast();
            BlockHelper.assertReverseSequence(blockIterator, reverseEntries.subList(1, reverseEntries.size()));
        }
        BlockHelper.assertSequence(blockIterator, entries);

        blockIterator.seekToEnd();
        BlockHelper.assertReverseSequence(blockIterator, reverseEntries);

        for (BlockEntry<ByteBuffer> entry : entries) {
            List<BlockEntry<ByteBuffer>> nextEntries = entries.subList(entries.indexOf(entry), entries.size());
            List<BlockEntry<ByteBuffer>> prevEntries = reverseEntries.subList(reverseEntries.indexOf(entry),
                    reverseEntries.size());
            blockIterator.seek(entry.getKey());
            BlockHelper.assertSequence(blockIterator, nextEntries);

            blockIterator.seek(BlockHelper.before(entry));
            BlockHelper.assertSequence(blockIterator, nextEntries);

            blockIterator.seek(BlockHelper.after(entry));
            BlockHelper.assertSequence(blockIterator, nextEntries.subList(1, nextEntries.size()));

            blockIterator.seek(BlockHelper.before(entry));
            BlockHelper.assertReverseSequence(blockIterator, prevEntries.subList(1, prevEntries.size()));

            blockIterator.seek(entry.getKey());
            BlockHelper.assertReverseSequence(blockIterator, prevEntries.subList(1, prevEntries.size()));

            blockIterator.seek(BlockHelper.after(entry));
            BlockHelper.assertReverseSequence(blockIterator, prevEntries);
        }

        blockIterator.seek(ByteBuffer.wrap(new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF }));
        BlockHelper.assertSequence(blockIterator, Collections.<BlockEntry<ByteBuffer>> emptyList());
        BlockHelper.assertReverseSequence(blockIterator, reverseEntries);
    }
}
