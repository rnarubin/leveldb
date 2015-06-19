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

import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.ReverseSeekingIterator;
import org.iq80.leveldb.impl.SeekingIterator;
import org.iq80.leveldb.impl.SequenceNumber;
import org.iq80.leveldb.impl.TransientInternalKey;
import org.iq80.leveldb.impl.ValueType;
import org.iq80.leveldb.util.ByteBuffers;
import org.testng.Assert;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import static com.google.common.base.Charsets.UTF_8;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_BYTE;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_INT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public final class BlockHelper
{
    private BlockHelper()
    {
    }

    public static int estimateBlockSize(int blockRestartInterval, List<BlockEntry> entries)
    {
        if (entries.isEmpty()) {
            return SIZE_OF_INT;
        }
        int restartCount = (int) Math.ceil(1.0 * entries.size() / blockRestartInterval);
        return estimateEntriesSize(blockRestartInterval, entries) +
                (restartCount * SIZE_OF_INT) +
                SIZE_OF_INT;
    }

    @SafeVarargs
    public static <K, V> void assertSequence(SeekingIterator<K, V> seekingIterator, Entry<K, V>... entries)
    {
        assertSequence(seekingIterator, Arrays.asList(entries));
    }

    public static <K, V> void assertSequence(SeekingIterator<K, V> seekingIterator, Iterable<? extends Entry<K, V>> entries)
    {
        Assert.assertNotNull(seekingIterator, "blockIterator is not null");

        for (Entry<K, V> entry : entries) {
            assertTrue(seekingIterator.hasNext());
            assertEntryEquals(seekingIterator.peek(), entry);
            assertEntryEquals(seekingIterator.next(), entry);
        }
        assertFalse(seekingIterator.hasNext());

        try {
            seekingIterator.peek();
            fail("expected NoSuchElementException");
        }
        catch (NoSuchElementException expected) {
        }
        try {
            seekingIterator.next();
            fail("expected NoSuchElementException");
        }
        catch (NoSuchElementException expected) {
        }
    }

    public static <K, V> void assertReverseSequence(ReverseSeekingIterator<K, V> rSeekingIterator, Iterable<? extends Entry<K, V>> reversedEntries)
    {
        for (Entry<K, V> entry : reversedEntries) {
            assertTrue(rSeekingIterator.hasPrev());
            assertEntryEquals(rSeekingIterator.peekPrev(), entry);
            assertEntryEquals(rSeekingIterator.prev(), entry);
        }
        assertFalse(rSeekingIterator.hasPrev());

        try {
            rSeekingIterator.peekPrev();
            fail("expected NoSuchElementException");
        }
        catch (NoSuchElementException expected) {
        }
        try {
            rSeekingIterator.prev();
            fail("expected NoSuchElementException");
        }
        catch (NoSuchElementException expected) {
        }
    }

    public static <K, V> void assertEntryEquals(Entry<K, V> actual, Entry<K, V> expected)
    {
        if (actual.getKey() instanceof ByteBuffer) {
            assertByteBufferEquals((ByteBuffer) actual.getKey(), (ByteBuffer) expected.getKey());
            assertByteBufferEquals((ByteBuffer) actual.getValue(), (ByteBuffer) expected.getValue());
        }
        assertEquals(actual, expected);
    }

    public static void assertByteBufferEquals(ByteBuffer actual, ByteBuffer expected)
    {
        assertEquals(actual, expected);
    }

    public static String beforeString(Entry<String, ?> expectedEntry)
    {
        String key = expectedEntry.getKey();
        int lastByte = key.charAt(key.length() - 1);
        return key.substring(0, key.length() - 1) + ((char) (lastByte - 1));
    }

    public static String afterString(Entry<String, ?> expectedEntry)
    {
        String key = expectedEntry.getKey();
        int lastByte = key.charAt(key.length() - 1);
        return key.substring(0, key.length() - 1) + ((char) (lastByte + 1));
    }

    public static InternalKey before(Entry<InternalKey, ?> expectedEntry)
    {
        ByteBuffer slice = ByteBuffers.heapCopy(expectedEntry.getKey().getUserKey());
        int lastByte = slice.limit() - 1;
        slice.put(lastByte, (byte) (slice.get(lastByte) - 1));
        return new TransientInternalKey(slice, SequenceNumber.MAX_SEQUENCE_NUMBER, ValueType.VALUE);
    }

    public static InternalKey after(Entry<InternalKey, ?> expectedEntry)
    {
        ByteBuffer slice = ByteBuffers.heapCopy(expectedEntry.getKey().getUserKey());
        int lastByte = slice.limit() - 1;
        slice.put(lastByte, (byte) (slice.get(lastByte) + 1));
        return new TransientInternalKey(slice, SequenceNumber.MAX_SEQUENCE_NUMBER, ValueType.VALUE);
    }

    public static int estimateEntriesSize(int blockRestartInterval, List<BlockEntry> entries)
    {
        int size = 0;
        ByteBuffer previousKey = null;
        int restartBlockCount = 0;
        for (BlockEntry entry : entries) {
            int nonSharedBytes;
            int rem = entry.getKey().getUserKey().remaining();
            if (restartBlockCount < blockRestartInterval) {
                nonSharedBytes = previousKey == null ? rem : rem
                        - ByteBuffers.calculateSharedBytes(entry.getKey().getUserKey(), previousKey);
            }
            else {
                nonSharedBytes = rem;
                restartBlockCount = 0;
            }
            size += nonSharedBytes + entry.getValue().remaining() + (SIZE_OF_BYTE * 3); // 3 bytes for sizes

            previousKey = entry.getKey().getUserKey();
            restartBlockCount++;
        }
        return size;
    }

    static BlockEntry createBlockEntry(String key, String value)
    {
        return new BlockEntry(new TransientInternalKey(ByteBuffer.wrap(key.getBytes(UTF_8)), 0, ValueType.VALUE),
                ByteBuffer.wrap(value.getBytes(UTF_8)));
    }
}
