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
import static org.iq80.leveldb.util.Iterators.Direction.FORWARD;
import static org.iq80.leveldb.util.Iterators.Direction.REVERSE;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_BYTE;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_INT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.UnaryOperator;

import org.iq80.leveldb.SeekingAsynchronousIterator;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.ReverseIterator;
import org.iq80.leveldb.impl.ReverseSeekingIterator;
import org.iq80.leveldb.impl.SeekingIterator;
import org.iq80.leveldb.impl.SequenceNumber;
import org.iq80.leveldb.impl.TransientInternalKey;
import org.iq80.leveldb.impl.ValueType;
import org.iq80.leveldb.util.ByteBuffers;
import org.iq80.leveldb.util.CompletableFutures;
import org.iq80.leveldb.util.Iterators;
import org.iq80.leveldb.util.Iterators.Direction;
import org.iq80.leveldb.util.MemoryManagers;
import org.testng.Assert;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

public final class TestHelper {
  private TestHelper() {}

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
      final SeekingAsynchronousIterator<K, V> iter, final Iterable<? extends Entry<K, V>> entries,
      final Executor asyncExec) {
    return assertSequence(iter, Direction.FORWARD, entries, asyncExec);
  }

  public static <K, V> CompletionStage<?> assertReverseSequence(
      final SeekingAsynchronousIterator<K, V> iter,
      final Iterable<? extends Entry<K, V>> reversedEntries, final Executor asyncExec) {
    return assertSequence(iter, Direction.REVERSE, reversedEntries, asyncExec);
  }

  public static <K, V> CompletionStage<?> assertSequence(
      final SeekingAsynchronousIterator<K, V> iter, final Direction direction,
      final Iterable<? extends Entry<K, V>> entries, final Executor asyncExec) {
    final Iterator<? extends Entry<K, V>> expected = entries.iterator();
    return CompletableFutures.flatMapIterator(iter, direction, entry -> {
      assertTrue(expected.hasNext(), "iterator contained more entries than expected");
      assertEntryEquals(entry, expected.next());
      return entry;
    } , asyncExec).thenApply(stream -> {
      assertFalse(expected.hasNext(), "iterator did not contain all expected entries");
      try {
        assertFalse(direction.asyncAdvance(iter).toCompletableFuture().get().isPresent());
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
    } else if (actual.getKey() instanceof ByteBuffer) {
      assertByteBufferEquals((ByteBuffer) actual.getKey(), (ByteBuffer) expected.getKey());
      assertByteBufferEquals((ByteBuffer) actual.getValue(), (ByteBuffer) expected.getValue());
    } else {
      assertEquals(actual, expected);
    }
  }

  public static void assertByteBufferEquals(final ByteBuffer actual, final ByteBuffer expected) {
    try {
      assertTrue(actual.compareTo(expected) == 0);
    } catch (final AssertionError e) {
      throw new AssertionError(String.format("bytebuffers not equal: expected [%s], actual [%s]",
          ByteBuffers.toString(expected), ByteBuffers.toString(actual)));
    }
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

  public static ByteBuffer before(final ByteBuffer b) {
    final ByteBuffer slice = ByteBuffers.heapCopy(b);
    final int lastByte = slice.limit() - 1;
    return slice.put(lastByte, (byte) (slice.get(lastByte) - 1));
  }

  public static ByteBuffer after(final ByteBuffer b) {
    final ByteBuffer slice = ByteBuffers.heapCopy(b);
    final int lastByte = slice.limit() - 1;
    return slice.put(lastByte, (byte) (slice.get(lastByte) + 1));
  }

  public static InternalKey before(final InternalKey key) {
    return new TransientInternalKey(before(key.getUserKey()), 0, ValueType.DELETION);
  }

  public static InternalKey after(final InternalKey key) {
    return new TransientInternalKey(after(key.getUserKey()), SequenceNumber.MAX_SEQUENCE_NUMBER,
        ValueType.VALUE);
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

  public static BlockEntry<ByteBuffer> createBlockEntry(final String key, final String value) {
    return BlockEntry.of(ByteBuffer.wrap(key.getBytes(UTF_8)),
        ByteBuffer.wrap(value.getBytes(UTF_8)));
  }

  public static Entry<InternalKey, ByteBuffer> createInternalEntry(final String key,
      final String value, final long sequenceNumber) {
    return Maps.<InternalKey, ByteBuffer>immutableEntry(
        new TransientInternalKey(ByteBuffer.wrap(key.getBytes(UTF_8)), sequenceNumber,
            ValueType.VALUE),
        ByteBuffer.wrap(value.getBytes(UTF_8)));
  }


  public static void testBufferIterator(
      final SeekingAsynchronousIterator<ByteBuffer, ByteBuffer> iter,
      final List<Entry<ByteBuffer, ByteBuffer>> entries, final Executor... asyncExec) {
    testIterator(iter, entries, ByteBuffer.wrap(new byte[] {0}),
        ByteBuffer.wrap(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}),
        TestHelper::before, TestHelper::after,
        asyncExec.length > 0 ? asyncExec[0] : ForkJoinPool.commonPool());
  }


  public static void testInternalKeyIterator(
      final SeekingAsynchronousIterator<InternalKey, ByteBuffer> iter,
      final List<Entry<InternalKey, ByteBuffer>> entries, final Executor... asyncExec) {
    testIterator(iter, entries,
        new TransientInternalKey(ByteBuffer.wrap(new byte[] {0}), Long.MAX_VALUE, ValueType.VALUE),
        new TransientInternalKey(
            ByteBuffer.wrap(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}), 0,
            ValueType.VALUE),
        TestHelper::before, TestHelper::after,
        asyncExec.length > 0 ? asyncExec[0] : ForkJoinPool.commonPool());
  }

  private static <K, V> void testIterator(final SeekingAsynchronousIterator<K, V> iter,
      final List<Entry<K, V>> entries, final K first, final K end, final UnaryOperator<K> before,
      final UnaryOperator<K> after, final Executor asyncExec) {
    final List<Entry<K, V>> reverseEntries = new ArrayList<>(entries);
    Collections.reverse(reverseEntries);

    try {
      iter.seekToFirst()
          .thenCompose(
              voided -> TestHelper.assertReverseSequence(iter, Collections.emptyList(), asyncExec))
          .thenCompose(voided -> TestHelper.assertSequence(iter, entries, asyncExec))
          .thenCompose(voided -> TestHelper.assertReverseSequence(iter, reverseEntries, asyncExec))
          .thenCompose(voided -> iter.seekToEnd())
          .thenCompose(
              voided -> TestHelper.assertSequence(iter, Collections.emptyList(), asyncExec))
          .thenCompose(voided -> TestHelper.assertReverseSequence(iter, reverseEntries, asyncExec))
          .thenCompose(voided -> TestHelper.assertSequence(iter, entries, asyncExec))
          .thenCompose(voided -> iter.seekToFirst())
          .thenCompose(voided -> TestHelper.assertSequence(iter, entries, asyncExec))
          .thenCompose(voided -> iter.seekToEnd())
          .thenCompose(voided -> TestHelper.assertReverseSequence(iter, reverseEntries, asyncExec))
          .toCompletableFuture().get();

      {
        int i = 0;
        for (final Entry<K, V> entry : entries) {
          final List<Entry<K, V>> nextEntries =
              entries.subList(entries.indexOf(entry), entries.size());
          final List<Entry<K, V>> prevEntries =
              reverseEntries.subList(entries.size() - i, entries.size());

          iter.seek(entry.getKey())
              .thenCompose(voided -> TestHelper.assertSequence(iter, nextEntries, asyncExec))
              .thenCompose(voided -> iter.seek(before.apply(entry.getKey())))
              .thenCompose(voided -> TestHelper.assertSequence(iter, nextEntries, asyncExec))
              .thenCompose(voided -> iter.seek(after.apply(entry.getKey())))
              .thenCompose(voided -> TestHelper.assertSequence(iter,
                  nextEntries.subList(1, nextEntries.size()), asyncExec))
              .toCompletableFuture().get();

          iter.seek(entry.getKey())
              .thenCompose(voided -> TestHelper.assertReverseSequence(iter, prevEntries, asyncExec))
              .thenCompose(voided -> iter.seek(before.apply(entry.getKey())))
              .thenCompose(voided -> TestHelper.assertReverseSequence(iter, prevEntries, asyncExec))
              .thenCompose(voided -> iter.seek(after.apply(entry.getKey())))
              .thenCompose(voided -> TestHelper.assertReverseSequence(iter,
                  Iterables.concat(Collections.singleton(nextEntries.get(0)), prevEntries),
                  asyncExec))
              .toCompletableFuture().get();

          i++;
        }
      }

      {
        for (int i = 1; i <= entries.size(); i++) {
          for (int j = 1; j <= i; j++) {
            for (int k = 1; k <= entries.size() - i + j; k++) {
              // i steps forward, j steps back, k steps forward
              final ReverseIterator<Entry<K, V>> expected = Iterators.listReverseIterator(entries);
              final int fi = i, fj = j, fk = k;
              iter.seekToFirst().thenCompose(voided -> step(FORWARD, iter, expected, fi))
                  .thenCompose(voided -> step(REVERSE, iter, expected, fj))
                  .thenCompose(voided -> step(FORWARD, iter, expected, fk)).toCompletableFuture()
                  .get();
            }
          }
        }
      }

      iter.seek(first)
          .thenCompose(
              voided -> TestHelper.assertReverseSequence(iter, Collections.emptyList(), asyncExec))
          .thenCompose(voided -> TestHelper.assertSequence(iter, entries, asyncExec))
          .thenCompose(voided -> iter.seek(first))
          .thenCompose(voided -> TestHelper.assertSequence(iter, entries, asyncExec))
          .toCompletableFuture().get();

      iter.seek(end)
          .thenCompose(
              voided -> TestHelper.assertSequence(iter, Collections.emptyList(), asyncExec))
          .thenCompose(voided -> TestHelper.assertReverseSequence(iter, reverseEntries, asyncExec))
          .thenCompose(voided -> iter.seek(end))
          .thenCompose(voided -> TestHelper.assertReverseSequence(iter, reverseEntries, asyncExec))
          .toCompletableFuture().get();

      iter.asyncClose().toCompletableFuture().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new AssertionError(e);
    }
  }

  private static <K, V> CompletionStage<Void> step(final Direction direction,
      final SeekingAsynchronousIterator<K, V> iter, final ReverseIterator<Entry<K, V>> expected,
      final int steps) {
    assert steps > 0;
    return direction.asyncAdvance(iter).thenCompose(optEntry -> {
      Assert.assertTrue(optEntry.isPresent(), "iterator did not contain all expected entries");
      Assert.assertTrue(direction.hasMore(expected));
      assertEntryEquals(optEntry.get(), direction.advance(expected));
      return steps == 1 ? CompletableFuture.completedFuture(null)
          : step(direction, iter, expected, steps - 1);
    });
  }
}
