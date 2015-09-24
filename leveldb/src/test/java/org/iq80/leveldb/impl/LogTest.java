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
package org.iq80.leveldb.impl;

import static com.google.common.base.Charsets.UTF_8;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import org.iq80.leveldb.FileInfo;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.util.ByteBuffers;
import org.iq80.leveldb.util.CompletableFutures;
import org.iq80.leveldb.util.EnvDependentTest;
import org.iq80.leveldb.util.FileEnvTestProvider;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public abstract class LogTest extends EnvDependentTest {

  private static final LogMonitor NO_CORRUPTION_MONITOR = LogMonitors.throwExceptionMonitor();
  private LogWriter writer;
  private FileInfo fileInfo;
  private Options options;

  @BeforeMethod
  public void setUp() throws InterruptedException, ExecutionException {
    options = Options.make().env(getEnv()).verifyChecksums(true);
    fileInfo = FileInfo.log(getHandle(), 42);
    writer = Logs.createLogWriter(fileInfo, 42, options).toCompletableFuture().get();
  }

  @AfterMethod
  public void tearDown() throws InterruptedException, ExecutionException {
    writer.asyncClose().toCompletableFuture().get();
    getEnv().fileExists(fileInfo).thenCompose(
        exists -> exists ? getEnv().deleteFile(fileInfo) : CompletableFuture.completedFuture(null))
        .toCompletableFuture().get();
  }

  @Test
  public void testEmptyBlock() {
    testLog();
  }

  @Test
  public void testSmallRecord() {
    testLog(toByteBuffer("dain sundstrom"));
  }

  @Test
  public void testMultipleSmallRecords() {
    testLog(toByteBuffer("Lagunitas  Little Sumpin’ Sumpin’"), toByteBuffer("Lagunitas IPA"),
        toByteBuffer("Lagunitas Imperial Stout"), toByteBuffer("Oban 14"),
        toByteBuffer("Highland Park"), toByteBuffer("Lagavulin"));
  }

  @Test
  public void testLargeRecord() {
    testLog(toByteBuffer("dain sundstrom", 4000));
  }

  @Test
  public void testMultipleLargeRecords() {
    testLog(toByteBuffer("Lagunitas  Little Sumpin’ Sumpin’", 4000),
        toByteBuffer("Lagunitas IPA", 4000), toByteBuffer("Lagunitas Imperial Stout", 4000),
        toByteBuffer("Oban 14", 4000), toByteBuffer("Highland Park", 4000),
        toByteBuffer("Lagavulin", 4000));
  }

  @Test
  public void testManySmallRecords() {
    final Random rand = new Random(0);
    final Stream.Builder<ByteBuffer> records = Stream.builder();
    for (int i = 0; i < 1_000_000; i++) {
      final byte[] b = new byte[rand.nextInt(20) + 5];
      rand.nextBytes(b);
      records.add(ByteBuffer.wrap(b));
    }
    testLog(records.build());
  }

  @Test
  public void testManyLargeRecords() {
    final Random rand = new Random(0);
    final Stream.Builder<ByteBuffer> records = Stream.builder();
    for (int i = 0; i < 10_000; i++) {
      final byte[] b = new byte[rand.nextInt(20) + 5];
      rand.nextBytes(b);
      records.add(repeat(b, rand.nextInt(2000) + 2000));
    }
    testLog(records.build());
  }

  @Test
  public void testManyHugeRecords() {
    final Random rand = new Random(0);
    final byte[] b = new byte[2 * 1024 * 1024];
    final ByteBuffer buf = ByteBuffer.wrap(b);

    final Stream.Builder<ByteBuffer> records = Stream.builder();
    for (int i = 0; i < 1000; i++) {
      records.add(ByteBuffers.duplicateByLength(buf, 0, rand.nextInt(1_000_000) + 1_000_000));
    }
    testLog(records.build());
  }

  @Test
  public void testReadWithoutProperClose() {
    testLog(Stream.of(toByteBuffer("something"), toByteBuffer("something else")), false);
  }

  private void testLog(final ByteBuffer... entries) {
    testLog(Stream.of(entries));
  }

  private void testLog(final Stream<ByteBuffer> records) {
    testLog(records, true);
  }

  private void testLog(final Stream<ByteBuffer> records, final boolean closeWriter) {
    try {
      CompletableFutures.allOf(records.map(buffer -> writer.addRecord(buffer, false)))
          .toCompletableFuture().get();

      if (closeWriter) {
        writer.asyncClose().toCompletableFuture().get();
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new AssertionError(e);
    }

    // getEnv().openSequentialReadFile(fileInfo)
    //
    // try (SequentialReadFile fileInput = getEnv().openSequentialReadFile(fileInfo);
    // StrictMemoryManager strictMemory = new StrictMemoryManager();
    // LogReader reader = new LogReader(fileInput, NO_CORRUPTION_MONITOR, true, 0, strictMemory)) {
    //
    // for (final ByteBuffer expected : records) {
    // final ByteBuffer actual = reader.readRecord();
    // assertEquals(actual, expected);
    // strictMemory.free(actual);
    // }
    // assertNull(reader.readRecord());
    // }
  }

  // @Test
  // public void testLogRecordBounds() throws Exception {
  // try {
  // final int recordSize = LogConstants.BLOCK_SIZE - LogConstants.HEADER_SIZE;
  // final ByteBuffer record = ByteBuffer.allocate(recordSize);
  //
  // final LogWriter writer = Logs.createLogWriter(fileInfo, 10, getOptions());
  // writer.addRecord(record, true);
  // writer.close();
  //
  // final LogMonitor logMonitor = new AssertNoCorruptionLogMonitor();
  //
  // try (StrictMemoryManager strictMemory = new StrictMemoryManager();
  // SequentialReadFile fileInput = getEnv().openSequentialReadFile(fileInfo);
  // LogReader logReader = new LogReader(fileInput, logMonitor, true, 0, strictMemory)) {
  //
  // int count = 0;
  // for (ByteBuffer slice = logReader.readRecord(); slice != null; slice =
  // logReader.readRecord()) {
  // assertEquals(slice.remaining(), recordSize);
  // count++;
  // strictMemory.free(slice);
  // }
  // assertEquals(count, 1);
  // }
  // } finally {
  // getEnv().deleteFile(fileInfo);
  // }
  // }

  static ByteBuffer toByteBuffer(final String value) {
    return toByteBuffer(value, 1);
  }

  static ByteBuffer toByteBuffer(final String value, final int times) {
    return repeat(value.getBytes(UTF_8), times);
  }

  static ByteBuffer repeat(final byte[] b, final int times) {
    final ByteBuffer ret = ByteBuffer.allocate(b.length * times);
    for (int i = 0; i < times; i++) {
      ret.put(b);
    }
    ret.flip();
    return ret;
  }

  public static class FileLogTest extends LogTest implements FileEnvTestProvider {
  }
}
