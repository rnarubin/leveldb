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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import org.iq80.leveldb.DBBufferComparator;
import org.iq80.leveldb.Env.SequentialWriteFile;
import org.iq80.leveldb.FileInfo;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.InternalKeyComparator;
import org.iq80.leveldb.impl.TransientInternalKey;
import org.iq80.leveldb.impl.ValueType;
import org.iq80.leveldb.util.ByteBuffers;
import org.iq80.leveldb.util.CompletableFutures;
import org.iq80.leveldb.util.EnvDependentTest;
import org.iq80.leveldb.util.FileEnvTestProvider;
import org.iq80.leveldb.util.MemoryManagers;
import org.iq80.leveldb.util.Snappy;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public abstract class TableTest extends EnvDependentTest {

  private static final DBBufferComparator byteCompare = new BytewiseComparator();
  private FileInfo fileInfo;

  @BeforeMethod
  public void setUp() throws Exception {
    fileInfo = FileInfo.table(getHandle(), 42);
  }

  private void clearFile() throws InterruptedException, ExecutionException {
    getEnv().fileExists(fileInfo).thenCompose(
        exists -> exists ? getEnv().deleteFile(fileInfo) : CompletableFuture.completedFuture(null))
        .toCompletableFuture().get();
  }

  @AfterMethod
  public void tearDown() throws Exception {
    clearFile();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testEmptyFile() throws Throwable {
    // create empty file if it doesn't exist
    getEnv().openSequentialWriteFile(fileInfo)
        .thenCompose(
            file -> CompletableFutures.composeUnconditionally(file.write(ByteBuffers.EMPTY_BUFFER),
                ignored -> file.asyncClose()))
        .toCompletableFuture().get();

    try {
      getEnv().openRandomReadFile(fileInfo)
          .thenCompose(
              file -> CompletableFutures.composeUnconditionally(Table
                  .newTable(file, new InternalKeyComparator(byteCompare),
                      Options.make().env(getEnv()).bufferComparator(byteCompare)
                          .verifyChecksums(true).compression(Snappy.instance()))
              .thenCompose(Table::release), voided -> file.asyncClose()))
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
        TestHelper.createInternalEntry("name", "dain sundstrom", 0));
  }

  @Test
  public void testMultipleEntriesWithSingleBlock() throws Exception {
    long seq = 0;
    final List<Entry<InternalKey, ByteBuffer>> entries = asList(
        TestHelper.createInternalEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’", seq++),
        TestHelper.createInternalEntry("beer/ipa", "Lagunitas IPA", seq++),
        TestHelper.createInternalEntry("beer/stout", "Lagunitas Imperial Stout", seq++),
        TestHelper.createInternalEntry("scotch/light", "Oban 14", seq++),
        TestHelper.createInternalEntry("scotch/medium", "Highland Park", seq++),
        TestHelper.createInternalEntry("scotch/strong", "Lagavulin", seq++));

    for (int i = 1; i < entries.size(); i++) {
      tableTest(Integer.MAX_VALUE, i, entries);
    }
  }

  @Test
  public void testMultipleEntriesWithMultipleBlock() throws Exception {
    long seq = 0;
    final List<Entry<InternalKey, ByteBuffer>> entries = asList(
        TestHelper.createInternalEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’", seq++),
        TestHelper.createInternalEntry("beer/ipa", "Lagunitas IPA", seq++),
        TestHelper.createInternalEntry("beer/stout", "Lagunitas Imperial Stout", seq++),
        TestHelper.createInternalEntry("scotch/light", "Oban 14", seq++),
        TestHelper.createInternalEntry("scotch/medium", "Highland Park", seq++),
        TestHelper.createInternalEntry("scotch/strong", "Lagavulin", seq++));

    // one entry per block
    tableTest(1, Integer.MAX_VALUE, entries);

    // about 3 blocks
    tableTest(TestHelper.estimateBlockSizeInternalKey(Integer.MAX_VALUE, entries) / 3,
        Integer.MAX_VALUE, entries);
  }

  @SafeVarargs
  private final void tableTest(final int blockSize, final int blockRestartInterval,
      final Entry<InternalKey, ByteBuffer>... entries) throws Exception {
    tableTest(blockSize, blockRestartInterval, asList(entries));
  }

  private void tableTest(final int blockSize, final int blockRestartInterval,
      final List<Entry<InternalKey, ByteBuffer>> entries) throws Exception {

    clearFile();
    final Options options = Options.make().blockSize(blockSize)
        .blockRestartInterval(blockRestartInterval).memoryManager(MemoryManagers.heap())
        .compression(Snappy.instance()).bufferComparator(byteCompare).env(getEnv());

    final SequentialWriteFile writeFile =
        getEnv().openSequentialWriteFile(fileInfo).toCompletableFuture().get();

    try (
        final TableBuilder builder = new TableBuilder(options, writeFile,
            new InternalKeyComparator(options.bufferComparator()));
        final AutoCloseable c = () -> writeFile.asyncClose().toCompletableFuture().get()) {

      final Iterator<Entry<InternalKey, ByteBuffer>> iter = entries.iterator();
      CompletionStage<Void> last = CompletableFuture.completedFuture(null);

      while (iter.hasNext()) {
        final Entry<InternalKey, ByteBuffer> entry = iter.next();
        last = last.thenCompose(voided -> builder.add(entry.getKey(), entry.getValue()));
      }

      last.thenCompose(voided -> builder.finish()).toCompletableFuture().get();
    }

    final Table table =
        Table.newTable(getEnv().openRandomReadFile(fileInfo).toCompletableFuture().get(),
            new InternalKeyComparator(byteCompare), options).toCompletableFuture().get();


    final TableIterator iter = table.retain().iterator();
    TestHelper.testInternalKeyIterator(iter, entries);

    long lastApproximateOffset = 0;
    for (final Entry<InternalKey, ByteBuffer> entry : entries) {
      final long approximateOffset = table.getApproximateOffsetOf(entry.getKey());
      Assert.assertTrue(approximateOffset >= lastApproximateOffset);
      lastApproximateOffset = approximateOffset;
    }

    final InternalKey endKey = new TransientInternalKey(
        ByteBuffer.wrap(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}), 0,
        ValueType.VALUE);
    final long approximateOffset = table.getApproximateOffsetOf(endKey);
    Assert.assertTrue(approximateOffset >= lastApproximateOffset);

    table.release().toCompletableFuture().get();

  }


  public static class FileTableTest extends TableTest implements FileEnvTestProvider {
  }

}
