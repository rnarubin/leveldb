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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import org.iq80.leveldb.DBBufferComparator;
import org.iq80.leveldb.Env;
import org.iq80.leveldb.Env.DBHandle;
import org.iq80.leveldb.Env.SequentialWriteFile;
import org.iq80.leveldb.FileInfo;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.FileEnv;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.InternalKeyComparator;
import org.iq80.leveldb.impl.ReverseSeekingIterator;
import org.iq80.leveldb.impl.TransientInternalKey;
import org.iq80.leveldb.impl.ValueType;
import org.iq80.leveldb.table.TableBuilder.BuilderState;
import org.iq80.leveldb.util.CompletableFutures;
import org.iq80.leveldb.util.MemoryManagers;
import org.iq80.leveldb.util.Snappy;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;

public abstract class TableTest {
  private static final DBBufferComparator byteCompare = new BytewiseComparator();
  private FileInfo fileInfo;
  private Env env;

  protected abstract Entry<? extends Env, ? extends DBHandle> createTempDB() throws Exception;

  @BeforeMethod
  public void setUp() throws Exception {
    final Entry<? extends Env, ? extends DBHandle> db = createTempDB();
    env = db.getKey();
    fileInfo = FileInfo.table(db.getValue(), 42);
  }

  private void clearFile() throws InterruptedException, ExecutionException {
    env.fileExists(fileInfo)
        .thenCompose(
            exists -> exists ? env.deleteFile(fileInfo) : CompletableFuture.completedFuture(null))
        .toCompletableFuture().get();
  }

  @AfterMethod
  public void tearDown() throws Exception {
    clearFile();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testEmptyFile() throws Throwable {
    try {
      env.openRandomReadFile(fileInfo)
          .thenCompose(file -> CompletableFutures.composeUnconditionally(
              Table.newTable(null, new InternalKeyComparator(byteCompare),
                  Options.make().env(env).bufferComparator(byteCompare).verifyChecksums(true)
                      .compression(Snappy.instance()))
                  .thenCompose(Table::release),
              voided -> file.asyncClose()))
          .toCompletableFuture().get();
    } catch (final ExecutionException e) {
      throw e.getCause();
    }
  }

  @Test
  public void testEmptyBlock() throws Exception {
    tableTest(Integer.MAX_VALUE, Integer.MAX_VALUE);
  }

  @Test
  public void testSingleEntrySingleBlock() throws Exception {
    tableTest(Integer.MAX_VALUE, Integer.MAX_VALUE,
        BlockHelper.createInternalEntry("name", "dain sundstrom", 0));
  }

  @Test
  public void testMultipleEntriesWithSingleBlock() throws Exception {
    long seq = 0;
    final List<Entry<InternalKey, ByteBuffer>> entries = asList(
        BlockHelper.createInternalEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’", seq++),
        BlockHelper.createInternalEntry("beer/ipa", "Lagunitas IPA", seq++),
        BlockHelper.createInternalEntry("beer/stout", "Lagunitas Imperial Stout", seq++),
        BlockHelper.createInternalEntry("scotch/light", "Oban 14", seq++),
        BlockHelper.createInternalEntry("scotch/medium", "Highland Park", seq++),
        BlockHelper.createInternalEntry("scotch/strong", "Lagavulin", seq++));

    for (int i = 1; i < entries.size(); i++) {
      tableTest(Integer.MAX_VALUE, i, entries);
    }
  }

  @Test
  public void testMultipleEntriesWithMultipleBlock() throws Exception {
    long seq = 0;
    final List<Entry<InternalKey, ByteBuffer>> entries = asList(
        BlockHelper.createInternalEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’", seq++),
        BlockHelper.createInternalEntry("beer/ipa", "Lagunitas IPA", seq++),
        BlockHelper.createInternalEntry("beer/stout", "Lagunitas Imperial Stout", seq++),
        BlockHelper.createInternalEntry("scotch/light", "Oban 14", seq++),
        BlockHelper.createInternalEntry("scotch/medium", "Highland Park", seq++),
        BlockHelper.createInternalEntry("scotch/strong", "Lagavulin", seq++));

    // one entry per block
    tableTest(1, Integer.MAX_VALUE, entries);

    // about 3 blocks
    tableTest(BlockHelper.estimateBlockSizeInternalKey(Integer.MAX_VALUE, entries) / 3,
        Integer.MAX_VALUE, entries);
  }

  @SafeVarargs
  private final void tableTest(final int blockSize, final int blockRestartInterval,
      final Entry<InternalKey, ByteBuffer>... entries) throws Exception {
    tableTest(blockSize, blockRestartInterval, asList(entries));
  }

  private void tableTest(final int blockSize, final int blockRestartInterval,
      final List<Entry<InternalKey, ByteBuffer>> entries) throws Exception {
    final List<Entry<InternalKey, ByteBuffer>> reverseEntries = new ArrayList<>(entries);
    Collections.reverse(reverseEntries);

    clearFile();
    final Options options = Options.make().blockSize(blockSize)
        .blockRestartInterval(blockRestartInterval).memoryManager(MemoryManagers.heap())
        .compression(Snappy.instance()).bufferComparator(byteCompare).env(env);

    final SequentialWriteFile writeFile =
        env.openSequentialWriteFile(fileInfo).toCompletableFuture().get();

    try (
        TableBuilder builder = new TableBuilder(options, writeFile,
            new InternalKeyComparator(options.bufferComparator()));
        AutoCloseable c = () -> writeFile.asyncClose().toCompletableFuture().get()) {

      final Iterator<Entry<InternalKey, ByteBuffer>> iter = entries.iterator();
      CompletionStage<BuilderState> last = CompletableFuture.completedFuture(builder.init());

      while (iter.hasNext()) {
        final Entry<InternalKey, ByteBuffer> entry = iter.next();
        last = last.thenCompose(state -> builder.add(entry.getKey(), entry.getValue(), state));
      }

      last.thenCompose(builder::finish).toCompletableFuture().get();
    }

    final Table table = Table.newTable(env.openRandomReadFile(fileInfo).toCompletableFuture().get(),
        new InternalKeyComparator(byteCompare), options).toCompletableFuture().get();


    try (TableIterator tableIter = table.retain().iterator()) {
      final ReverseSeekingIterator<InternalKey, ByteBuffer> seekingIterator = tableIter;

      seekingIterator.seekToFirst();
      BlockHelper.assertReverseSequence(seekingIterator,
          Collections.<Entry<InternalKey, ByteBuffer>>emptyList());
      BlockHelper.assertSequence(seekingIterator, entries);
      BlockHelper.assertReverseSequence(seekingIterator, reverseEntries);

      seekingIterator.seekToEnd();
      BlockHelper.assertSequence(seekingIterator,
          Collections.<Entry<InternalKey, ByteBuffer>>emptyList());
      BlockHelper.assertReverseSequence(seekingIterator, reverseEntries);
      BlockHelper.assertSequence(seekingIterator, entries);

      long lastApproximateOffset = 0;
      for (final Entry<InternalKey, ByteBuffer> entry : entries) {
        final List<Entry<InternalKey, ByteBuffer>> nextEntries =
            entries.subList(entries.indexOf(entry), entries.size());
        seekingIterator.seek(entry.getKey());
        BlockHelper.assertSequence(seekingIterator, nextEntries);

        seekingIterator.seek(BlockHelper.beforeInternalKey(entry));
        BlockHelper.assertSequence(seekingIterator, nextEntries);

        seekingIterator.seek(BlockHelper.afterInternalKey(entry));
        BlockHelper.assertSequence(seekingIterator, nextEntries.subList(1, nextEntries.size()));

        final long approximateOffset = table.getApproximateOffsetOf(entry.getKey());
        Assert.assertTrue(approximateOffset >= lastApproximateOffset);
        lastApproximateOffset = approximateOffset;
      }

      final InternalKey endKey = new TransientInternalKey(
          ByteBuffer.wrap(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}), 0,
          ValueType.VALUE);
      seekingIterator.seek(endKey);
      BlockHelper.assertSequence(seekingIterator, Collections.<BlockEntry<InternalKey>>emptyList());
      BlockHelper.assertReverseSequence(seekingIterator, reverseEntries);

      final long approximateOffset = table.getApproximateOffsetOf(endKey);
      Assert.assertTrue(approximateOffset >= lastApproximateOffset);
    }

    table.release().toCompletableFuture().get();

  }

  public static class FileTableTest extends TableTest {

    @Override
    protected Entry<Env, DBHandle> createTempDB()
        throws IOException, InterruptedException, ExecutionException {
      final Path tempDir = Files.createTempDirectory("leveldb-testing");
      final FileEnv env = new FileEnv();
      return Maps.immutableEntry(env,
          env.createDB(Optional.of(FileEnv.handle(tempDir))).toCompletableFuture().get());
    }

  }

  /*
   * public static class FileChannelTableTest extends TableTest { private StrictEnv env;
   *
   * @BeforeMethod public void setupEnv() { env = new StrictEnv(); }
   *
   * @AfterMethod public void tearDownEnv() throws IOException { env.close(); }
   *
   * @Override protected Env getEnv() { return env; } }
   */
}
