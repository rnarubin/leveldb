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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.LongToIntFunction;
import java.util.function.UnaryOperator;

import org.iq80.leveldb.AsynchronousCloseable;
import org.iq80.leveldb.Compression;
import org.iq80.leveldb.DBBufferComparator;
import org.iq80.leveldb.Env;
import org.iq80.leveldb.Env.DBHandle;
import org.iq80.leveldb.Env.DelegateEnv;
import org.iq80.leveldb.Env.SequentialWriteFile;
import org.iq80.leveldb.FileInfo;
import org.iq80.leveldb.SeekingAsynchronousIterator;
import org.iq80.leveldb.impl.FileMetaData;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.InternalKeyComparator;
import org.iq80.leveldb.impl.ReverseIterator;
import org.iq80.leveldb.impl.ReverseSeekingIterator;
import org.iq80.leveldb.impl.SeekingIterator;
import org.iq80.leveldb.impl.SequenceNumber;
import org.iq80.leveldb.impl.TableCache;
import org.iq80.leveldb.impl.TransientInternalKey;
import org.iq80.leveldb.impl.ValueType;
import org.iq80.leveldb.util.ByteBuffers;
import org.iq80.leveldb.util.CompletableFutures;
import org.iq80.leveldb.util.Iterators;
import org.iq80.leveldb.util.Iterators.Direction;
import org.iq80.leveldb.util.MemoryManagers;
import org.iq80.leveldb.util.Snappy;
import org.testng.Assert;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

public final class TestUtils {
  public static final DBBufferComparator byteComparator = new BytewiseComparator();
  public static final InternalKeyComparator keyComparator =
      new InternalKeyComparator(byteComparator);

  private TestUtils() {}

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

  public static ByteBuffer toBuf(final String s) {
    return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
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

  public static InternalKey createInternalKey(final String key, final long sequenceNumber) {
    return new TransientInternalKey(ByteBuffer.wrap(key.getBytes(UTF_8)), sequenceNumber,
        ValueType.VALUE);
  }

  public static Entry<InternalKey, ByteBuffer> createInternalEntry(final String key,
      final String value, final long sequenceNumber) {
    return Maps.<InternalKey, ByteBuffer>immutableEntry(createInternalKey(key, sequenceNumber),
        ByteBuffer.wrap(value.getBytes(UTF_8)));
  }


  public static void testBufferIterator(
      final SeekingAsynchronousIterator<ByteBuffer, ByteBuffer> iter,
      final List<Entry<ByteBuffer, ByteBuffer>> entries, final Executor... asyncExec) {
    testIterator(iter, entries, ByteBuffer.wrap(new byte[] {0}),
        ByteBuffer.wrap(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}),
        TestUtils::before, TestUtils::after,
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
        TestUtils::before, TestUtils::after,
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
              voided -> TestUtils.assertReverseSequence(iter, Collections.emptyList(), asyncExec))
          .thenCompose(voided -> TestUtils.assertSequence(iter, entries, asyncExec))
          .thenCompose(voided -> TestUtils.assertReverseSequence(iter, reverseEntries, asyncExec))
          .thenCompose(voided -> iter.seekToEnd())
          .thenCompose(voided -> TestUtils.assertSequence(iter, Collections.emptyList(), asyncExec))
          .thenCompose(voided -> TestUtils.assertReverseSequence(iter, reverseEntries, asyncExec))
          .thenCompose(voided -> TestUtils.assertSequence(iter, entries, asyncExec))
          .thenCompose(voided -> iter.seekToFirst())
          .thenCompose(voided -> TestUtils.assertSequence(iter, entries, asyncExec))
          .thenCompose(voided -> iter.seekToEnd())
          .thenCompose(voided -> TestUtils.assertReverseSequence(iter, reverseEntries, asyncExec))
          .toCompletableFuture().get();

      {
        int i = 0;
        for (final Entry<K, V> entry : entries) {
          final List<Entry<K, V>> nextEntries =
              entries.subList(entries.indexOf(entry), entries.size());
          final List<Entry<K, V>> prevEntries =
              reverseEntries.subList(entries.size() - i, entries.size());

          iter.seek(entry.getKey())
              .thenCompose(voided -> TestUtils.assertSequence(iter, nextEntries, asyncExec))
              .thenCompose(voided -> iter.seek(before.apply(entry.getKey())))
              .thenCompose(voided -> TestUtils.assertSequence(iter, nextEntries, asyncExec))
              .thenCompose(voided -> iter.seek(after.apply(entry.getKey())))
              .thenCompose(voided -> TestUtils.assertSequence(iter,
                  nextEntries.subList(1, nextEntries.size()), asyncExec))
              .toCompletableFuture().get();

          iter.seek(entry.getKey())
              .thenCompose(voided -> TestUtils.assertReverseSequence(iter, prevEntries, asyncExec))
              .thenCompose(voided -> iter.seek(before.apply(entry.getKey())))
              .thenCompose(voided -> TestUtils.assertReverseSequence(iter, prevEntries, asyncExec))
              .thenCompose(voided -> iter.seek(after.apply(entry.getKey())))
              .thenCompose(voided -> TestUtils.assertReverseSequence(iter,
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
              voided -> TestUtils.assertReverseSequence(iter, Collections.emptyList(), asyncExec))
          .thenCompose(voided -> TestUtils.assertSequence(iter, entries, asyncExec))
          .thenCompose(voided -> iter.seek(first))
          .thenCompose(voided -> TestUtils.assertSequence(iter, entries, asyncExec))
          .toCompletableFuture().get();

      iter.seek(end)
          .thenCompose(voided -> TestUtils.assertSequence(iter, Collections.emptyList(), asyncExec))
          .thenCompose(voided -> TestUtils.assertReverseSequence(iter, reverseEntries, asyncExec))
          .thenCompose(voided -> iter.seek(end))
          .thenCompose(voided -> TestUtils.assertReverseSequence(iter, reverseEntries, asyncExec))
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

  public static FileMetaData buildTable(final Env env, final DBHandle db, final long fileNumber,
      final List<Entry<InternalKey, ByteBuffer>> entries, final int blockSize,
      final int blockRestartInterval, final Compression compression) throws Exception {
    return buildTable(env, FileInfo.table(db, fileNumber), entries, blockSize, blockRestartInterval,
        compression);
  }

  public static FileMetaData buildTable(final Env env, final FileInfo info,
      final List<Entry<InternalKey, ByteBuffer>> entries, final int blockSize,
      final int blockRestartInterval, final Compression compression) throws Exception {

    final SequentialWriteFile writeFile =
        env.openSequentialWriteFile(info).toCompletableFuture().get();

    try (final TableBuilder builder =
        new TableBuilder(blockRestartInterval, blockSize, compression, writeFile, keyComparator);
        final AutoCloseable c = autoCloseable(writeFile)) {

      CompletionStage<Void> last = CompletableFuture.completedFuture(null);
      for (final Entry<InternalKey, ByteBuffer> entry : entries) {
        last = last.thenCompose(voided -> builder.add(entry.getKey(), entry.getValue()));
      }

      final long fileSize =
          last.thenCompose(voided -> builder.finish()).toCompletableFuture().get();

      if (entries.isEmpty()) {
        return new FileMetaData(info.getFileNumber(), fileSize, null, null);
      } else {
        return new FileMetaData(info.getFileNumber(), fileSize, entries.get(0).getKey(),
            entries.get(entries.size() - 1).getKey());
      }
    }
  }

  public static LongSupplier counter(final long seed) {
    final AtomicLong i = new AtomicLong(seed);
    return () -> i.getAndIncrement();
  }

  public static Entry<TableCache, FileMetaData[]> generateTableCache(final Env env,
      final DBHandle db, final List<List<Entry<InternalKey, ByteBuffer>>> tableEntries,
      final LongSupplier fileNumbers, final int blockSize, final int blockRestartInterval,
      final int cacheSize) throws Exception {
    final Compression compression = Snappy.instance();

    final FileMetaData[] files = new FileMetaData[tableEntries.size()];
    int i = 0;
    for (final List<Entry<InternalKey, ByteBuffer>> entries : tableEntries) {
      files[i] = buildTable(env, db, fileNumbers.getAsLong(), entries, blockSize,
          blockRestartInterval, compression);
      i++;
    }

    final AtomicReference<Throwable> throwableHolder = new AtomicReference<>(null);
    return Maps.immutableEntry(new TableCache(db, cacheSize, keyComparator, env, true, compression,
        (thread, throwable) -> throwableHolder.set(throwable)) {
      @Override
      public CompletionStage<Void> asyncClose() {
        return super.asyncClose().thenAccept(voided -> {
          final Throwable t = throwableHolder.get();
          if (t != null) {
            throw new AssertionError(t);
          }
        });
      }
    }, files);
  }

  public static AutoCloseable autoCloseable(final AsynchronousCloseable c) {
    return () -> c.asyncClose().toCompletableFuture().get();
  }

  public static class StrictEnv extends DelegateEnv implements Closeable {
    private final Map<AsynchronousCloseable, MetaData> openMap =
        Collections.synchronizedMap(new IdentityHashMap<>());

    private class MetaData {
      public final Throwable stackHolder;

      private MetaData(final Throwable stackHolder) {
        this.stackHolder = stackHolder;
      }
    }

    public StrictEnv(final Env delegate) {
      super(delegate);
    }

    private void put(final AsynchronousCloseable c, final MetaData m) {
      openMap.put(c, m);
    }

    private void remove(final AsynchronousCloseable c) {
      if (openMap.remove(c) == null) {
        throw new IllegalStateException("removed nonexistant object");
      }
    }

    @Override
    public CompletionStage<? extends ConcurrentWriteFile> openConcurrentWriteFile(
        final FileInfo info) {
      final MetaData m = new MetaData(new Throwable());
      return super.openConcurrentWriteFile(info)
          .thenApply(file -> new DelegateConcurrentWriteFile(file) {
            {
              put(file, m);
            }

            @Override
            public CompletionStage<WriteRegion> requestRegion(final LongToIntFunction f) {
              return super.requestRegion(f).thenApply(region -> new DelegateWriteRegion(region) {
                {
                  // no stack holder as it's a fairly expensive operation for such a frequent call.
                  // not very useful either as regions are really only requested in one class
                  StrictEnv.this.put(region, new MetaData(null));
                }

                @Override
                public CompletionStage<Void> asyncClose() {
                  return super.asyncClose().thenAccept(voided -> remove(region));
                }
              });
            }

            @Override
            public CompletionStage<Void> asyncClose() {
              return super.asyncClose().thenAccept(voided -> remove(file));
            }
          });
    }

    @Override
    public CompletionStage<? extends SequentialWriteFile> openSequentialWriteFile(
        final FileInfo info) {
      final MetaData m = new MetaData(new Throwable());
      return super.openSequentialWriteFile(info)
          .thenApply(file -> new DelegateSequentialWriteFile(file) {
            {
              put(file, m);
            }

            @Override
            public CompletionStage<Void> asyncClose() {
              return super.asyncClose().thenAccept(voided -> remove(file));
            }
          });
    }

    @Override
    public CompletionStage<? extends TemporaryWriteFile> openTemporaryWriteFile(final FileInfo temp,
        final FileInfo target) {
      final MetaData m = new MetaData(new Throwable());
      return super.openTemporaryWriteFile(temp, target)
          .thenApply(file -> new DelegateTemporaryWriteFile(file) {
            {
              put(file, m);
            }

            @Override
            public CompletionStage<Void> asyncClose() {
              return super.asyncClose().thenAccept(voided -> remove(file));
            }
          });
    }

    @Override
    public CompletionStage<? extends SequentialReadFile> openSequentialReadFile(
        final FileInfo info) {
      final MetaData m = new MetaData(new Throwable());
      return super.openSequentialReadFile(info)
          .thenApply(file -> new DelegateSequentialReadFile(file) {
            {
              put(file, m);
            }

            @Override
            public CompletionStage<Void> asyncClose() {
              return super.asyncClose().thenAccept(voided -> remove(file));
            }
          });
    }

    @Override
    public CompletionStage<? extends RandomReadFile> openRandomReadFile(final FileInfo info) {
      final MetaData m = new MetaData(new Throwable());
      return super.openRandomReadFile(info).thenApply(file -> new DelegateRandomReadFile(file) {
        {
          put(file, m);
        }

        @Override
        public CompletionStage<Void> asyncClose() {
          return super.asyncClose().thenAccept(voided -> remove(file));
        }
      });
    }

    @Override
    public CompletionStage<? extends LockFile> lockFile(final FileInfo info) {
      final MetaData m = new MetaData(new Throwable());
      return super.lockFile(info).thenApply(file -> new DelegateLockFile(file) {
        {
          put(file, m);
        }

        @Override
        public CompletionStage<Void> asyncClose() {
          return super.asyncClose().thenAccept(voided -> remove(file));
        }
      });
    }

    @Override
    public CompletionStage<? extends AsynchronousCloseableIterator<FileInfo>> getOwnedFiles(
        final DBHandle handle) {
      final MetaData m = new MetaData(new Throwable());
      return super.getOwnedFiles(handle).<AsynchronousCloseableIterator<FileInfo>>thenApply(
          iter -> new AsynchronousCloseableIterator<FileInfo>() {
            {
              put(iter, m);
            }

            @Override
            public CompletionStage<Optional<FileInfo>> next() {
              return iter.next();
            }

            @Override
            public CompletionStage<Void> asyncClose() {
              return iter.asyncClose().thenAccept(voided -> remove(iter));
            }
          });
    }

    @Override
    public void close() throws IOException {
      if (!openMap.isEmpty()) {
        final Entry<AsynchronousCloseable, MetaData> openEntry =
            openMap.entrySet().iterator().next();
        throw new AssertionError("file not closed:" + openEntry.getKey(),
            openEntry.getValue().stackHolder);
      }
    }

  }
}
