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
package com.cleversafe.leveldb.impl;

import static com.google.common.base.Charsets.UTF_8;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.cleversafe.leveldb.FileInfo;
import com.cleversafe.leveldb.util.ByteBuffers;
import com.cleversafe.leveldb.util.CompletableFutures;
import com.cleversafe.leveldb.util.EnvDependentTest;
import com.cleversafe.leveldb.util.FileEnvTestProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AtomicLongMap;

public abstract class LogTest extends EnvDependentTest {

  private LogWriter writer;
  private boolean mustClose = true;
  private FileInfo fileInfo;

  @BeforeMethod
  public void setUp() throws InterruptedException, ExecutionException {
    fileInfo = FileInfo.log(getHandle(), 42);
    writer = Logs.createLogWriter(fileInfo, getEnv()).toCompletableFuture().get();
    mustClose = true;
  }

  @AfterMethod
  public void tearDown() throws InterruptedException, ExecutionException {
    if (mustClose) {
      writer.asyncClose().toCompletableFuture().get();
    }
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
    final ImmutableList.Builder<ByteBuffer> records = ImmutableList.builder();
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
    final ImmutableList.Builder<ByteBuffer> records = ImmutableList.builder();
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

    final ImmutableList.Builder<ByteBuffer> records = ImmutableList.builder();
    for (int i = 0; i < 1000; i++) {
      records.add(ByteBuffers.duplicateByLength(buf, 0, rand.nextInt(1_000_000) + 1_000_000));
    }
    testLog(records.build());
  }

  @Test
  public void testReadWithoutProperClose() {
    testLog(Arrays.asList(toByteBuffer("something"), toByteBuffer("something else")), false);
  }

  private void testLog(final ByteBuffer... entries) {
    testLog(Arrays.asList(entries));
  }

  private void testLog(final List<ByteBuffer> records) {
    testLog(records, true);
  }

  private void testLog(final List<ByteBuffer> records, final boolean closeWriter) {
    try {
      CompletableFutures.allOf(records.stream().map(buffer -> writer.addRecord(buffer, false)))
          .toCompletableFuture().get();

      if (closeWriter) {
        writer.asyncClose().toCompletableFuture().get();
        mustClose = false;
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new AssertionError(e);
    }

    final AtomicLongMap<ByteBuffer> recordCounts = AtomicLongMap.create(records.stream()
        .collect(Collectors.toMap(buffer -> buffer, buffer -> 1L, (x, y) -> x + y)));
    Assert.assertEquals(recordCounts.sum(), records.size());

    try {
      LogReader.newLogReader(getEnv(), fileInfo, LogMonitors.throwExceptionMonitor(), true, 0)
          .thenCompose(logReader -> CompletableFutures.<Stream<Long>, Void>composeUnconditionally(
              CompletableFutures.mapAndCollapse(logReader,
                  record -> recordCounts.decrementAndGet(record), getEnv().getExecutor()),
              ignored -> logReader.asyncClose()))
          .toCompletableFuture().get(30, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new AssertionError(e);
    }

    Assert.assertEquals(recordCounts.sum(), 0L);
  }

  @Test
  public void testLogRecordBounds() throws Exception {
    final int recordSize = LogConstants.BLOCK_SIZE - LogConstants.HEADER_SIZE;
    final ByteBuffer record = ByteBuffer.allocate(recordSize);

    writer.addRecord(record, true).thenCompose(voided -> writer.asyncClose()).toCompletableFuture()
        .get();
    mustClose = false;
    record.rewind();

    LogReader.newLogReader(getEnv(), fileInfo, LogMonitors.throwExceptionMonitor(), true, 0)
        .thenCompose(logReader -> logReader.next().thenCompose(optRecord -> {
          Assert.assertTrue(optRecord.isPresent());
          Assert.assertTrue(optRecord.get().equals(record));
          return logReader.next();
        }).thenCompose(optRecord -> {
          Assert.assertFalse(optRecord.isPresent());
          return logReader.asyncClose();
        })).toCompletableFuture().get();
  }

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
