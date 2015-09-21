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

import static com.google.common.base.Charsets.UTF_8;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_BYTE;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_INT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletionStage;

import org.iq80.leveldb.SeekingAsynchronousIterator;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.ReverseSeekingIterator;
import org.iq80.leveldb.impl.SeekingIterator;
import org.iq80.leveldb.impl.SequenceNumber;
import org.iq80.leveldb.impl.TransientInternalKey;
import org.iq80.leveldb.impl.ValueType;
import org.iq80.leveldb.util.ByteBuffers;
import org.iq80.leveldb.util.CompletableFutures;
import org.iq80.leveldb.util.Iterators.Direction;
import org.iq80.leveldb.util.MemoryManagers;
import org.testng.Assert;

import com.google.common.collect.Maps;

public final class BlockHelper {
  private BlockHelper() {}

  public static int estimateBlockSizeInternalKey(final int blockRestartInterval,
      final List<Entry<InternalKey, ByteBuffer>> entries) {
    final List<BlockEntry<ByteBuffer>> blockEntries = new ArrayList<>(entries.size());
    for (final Entry<InternalKey, ByteBuffer> entry : entries) {
      final ByteBuffer encoded = MemoryManagers.heap().allocate(entry.getKey().getEncodedSize());
      entry.getKey().writeToBuffer(encoded).flip();
      blockEntries.add(BlockEntry.of(encoded, entry.getValue()));
    }
    return estimateBlockSize(blockRestartInterval, blockEntries);
  }

  public static int estimateBlockSize(final int blockRestartInterval,
      final List<BlockEntry<ByteBuffer>> entries) {
    if (entries.isEmpty()) {
      return SIZE_OF_INT;
    }
    final int restartCount = (int) Math.ceil(1.0 * entries.size() / blockRestartInterval);
    return estimateEntriesSize(blockRestartInterval, entries) + (restartCount * SIZE_OF_INT)
        + SIZE_OF_INT;
  }

  @SafeVarargs
  public static <K, V> void assertSequence(final SeekingIterator<K, V> seekingIterator,
      final Entry<K, V>... entries) {
    assertSequence(seekingIterator, Arrays.asList(entries));
  }

  public static <K, V> void assertSequence(final SeekingIterator<K, V> seekingIterator,
      final Iterable<? extends Entry<K, V>> entries) {
    Assert.assertNotNull(seekingIterator, "blockIterator is not null");

    for (final Entry<K, V> entry : entries) {
      assertTrue(seekingIterator.hasNext());
      assertEntryEquals(seekingIterator.peek(), entry);
      assertEntryEquals(seekingIterator.next(), entry);
    }
    assertFalse(seekingIterator.hasNext());

    try {
      seekingIterator.peek();
      fail("expected NoSuchElementException");
    } catch (final NoSuchElementException expected) {
    }
    try {
      seekingIterator.next();
      fail("expected NoSuchElementException");
    } catch (final NoSuchElementException expected) {
    }
  }

  public static <K, V> CompletionStage<?> assertSequence(
      final SeekingAsynchronousIterator<K, V> iter, final Iterable<? extends Entry<K, V>> entries) {
    return assertSequence(iter, Direction.NEXT, entries);
  }

  public static <K, V> CompletionStage<?> assertReverseSequence(
      final SeekingAsynchronousIterator<K, V> iter,
      final Iterable<? extends Entry<K, V>> reversedEntries) {
    return assertSequence(iter, Direction.PREV, reversedEntries);
  }

  public static <K, V> CompletionStage<?> assertSequence(
      final SeekingAsynchronousIterator<K, V> iter, final Direction direction,
      final Iterable<? extends Entry<K, V>> entries) {
    final Iterator<? extends Entry<K, V>> expected = entries.iterator();
    return CompletableFutures.flatMapIterator(iter, direction, entry -> {
      assertTrue(expected.hasNext(), "more entries in iterator than expected");
      assertEntryEquals(entry, expected.next());
      return null;
    }).thenApply(stream -> {
      assertFalse(expected.hasNext());
      try {
        assertFalse(iter.next().toCompletableFuture().get().isPresent());
      } catch (final Exception e) {
        throw new AssertionError(e);
      }
      return null;
    });
  }

  public static <K, V> void assertReverseSequence(
      final ReverseSeekingIterator<K, V> rSeekingIterator,
      final Iterable<? extends Entry<K, V>> reversedEntries) {
    for (final Entry<K, V> entry : reversedEntries) {
      assertTrue(rSeekingIterator.hasPrev());
      assertEntryEquals(rSeekingIterator.peekPrev(), entry);
      assertEntryEquals(rSeekingIterator.prev(), entry);
    }
    assertFalse(rSeekingIterator.hasPrev());

    try {
      rSeekingIterator.peekPrev();
      fail("expected NoSuchElementException");
    } catch (final NoSuchElementException expected) {
    }
    try {
      rSeekingIterator.prev();
      fail("expected NoSuchElementException");
    } catch (final NoSuchElementException expected) {
    }
  }

  public static <K, V> void assertEntryEquals(final Entry<K, V> actual,
      final Entry<K, V> expected) {
    if (actual.getKey() instanceof InternalKey) {
      assertEquals(actual.getKey(), expected.getKey());
      assertByteBufferEquals((ByteBuffer) actual.getValue(), (ByteBuffer) expected.getValue());
    } else {
      assertEquals(actual, expected);
    }
  }

  public static void assertByteBufferEquals(final ByteBuffer actual, final ByteBuffer expected) {
    assertTrue(ByteBuffers.compare(actual, expected) == 0);
  }

  public static String beforeString(final Entry<String, ?> expectedEntry) {
    final String key = expectedEntry.getKey();
    final int lastByte = key.charAt(key.length() - 1);
    return key.substring(0, key.length() - 1) + ((char) (lastByte - 1));
  }

  public static String afterString(final Entry<String, ?> expectedEntry) {
    final String key = expectedEntry.getKey();
    final int lastByte = key.charAt(key.length() - 1);
    return key.substring(0, key.length() - 1) + ((char) (lastByte + 1));
  }

  public static InternalKey beforeInternalKey(final Entry<InternalKey, ?> expectedEntry) {
    return new TransientInternalKey(before(expectedEntry.getKey().getUserKey()),
        SequenceNumber.MAX_SEQUENCE_NUMBER, ValueType.VALUE);
  }

  public static ByteBuffer before(final Entry<ByteBuffer, ?> expectedEntry) {
    return before(expectedEntry.getKey());
  }

  public static ByteBuffer before(final ByteBuffer b) {
    final ByteBuffer slice = ByteBuffers.heapCopy(b);
    final int lastByte = slice.limit() - 1;
    return slice.put(lastByte, (byte) (slice.get(lastByte) - 1));
  }

  public static InternalKey afterInternalKey(final Entry<InternalKey, ?> expectedEntry) {
    return new TransientInternalKey(after(expectedEntry.getKey().getUserKey()),
        SequenceNumber.MAX_SEQUENCE_NUMBER, ValueType.VALUE);
  }

  public static ByteBuffer after(final Entry<ByteBuffer, ?> expectedEntry) {
    return after(expectedEntry.getKey());
  }

  public static ByteBuffer after(final ByteBuffer b) {
    final ByteBuffer slice = ByteBuffers.heapCopy(b);
    final int lastByte = slice.limit() - 1;
    return slice.put(lastByte, (byte) (slice.get(lastByte) + 1));
  }

  public static int estimateEntriesSize(final int blockRestartInterval,
      final List<BlockEntry<ByteBuffer>> entries) {
    int size = 0;
    ByteBuffer previousKey = null;
    int restartBlockCount = 0;
    for (final BlockEntry<ByteBuffer> entry : entries) {
      int nonSharedBytes;
      final int rem = entry.getKey().remaining();
      if (restartBlockCount < blockRestartInterval) {
        nonSharedBytes = previousKey == null ? rem
            : rem - ByteBuffers.calculateSharedBytes(entry.getKey(), previousKey);
      } else {
        nonSharedBytes = rem;
        restartBlockCount = 0;
      }
      size += nonSharedBytes + entry.getValue().remaining() + (SIZE_OF_BYTE * 3); // 3 bytes for
                                                                                  // sizes

      previousKey = entry.getKey();
      restartBlockCount++;
    }
    return size;
  }

  static BlockEntry<ByteBuffer> createBlockEntry(final String key, final String value) {
    return BlockEntry.of(ByteBuffer.wrap(key.getBytes(UTF_8)),
        ByteBuffer.wrap(value.getBytes(UTF_8)));
  }

  static Entry<InternalKey, ByteBuffer> createInternalEntry(final String key, final String value,
      final long sequenceNumber) {
    return Maps.<InternalKey, ByteBuffer>immutableEntry(
        new TransientInternalKey(ByteBuffer.wrap(key.getBytes(UTF_8)), sequenceNumber,
            ValueType.VALUE),
        ByteBuffer.wrap(value.getBytes(UTF_8)));
  }
}
