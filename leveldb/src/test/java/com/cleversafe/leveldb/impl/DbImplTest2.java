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

import static com.cleversafe.leveldb.impl.DbConstants.NUM_LEVELS;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.cleversafe.leveldb.Options;
import com.cleversafe.leveldb.ReadOptions;
import com.cleversafe.leveldb.Snapshot;
import com.cleversafe.leveldb.WriteBatch;
import com.cleversafe.leveldb.WriteOptions;
import com.cleversafe.leveldb.util.ByteBuffers;
import com.cleversafe.leveldb.util.EnvDependentTest;
import com.cleversafe.leveldb.util.FileEnvTestProvider;
import com.google.common.base.Throwables;

public abstract class DbImplTest2 extends EnvDependentTest {
  // You can set the STRESS_FACTOR system property to make the tests run more iterations.
  public static final double STRESS_FACTOR =
      Double.parseDouble(System.getProperty("STRESS_FACTOR", "1"));
  private static final String DOES_NOT_EXIST_FILENAME = "/foo/bar/doowop/idontexist";
  private String testName;

  @Test
  public void testEmpty() throws InterruptedException, ExecutionException {
    try (DbStringWrapper db = new DbStringWrapper(Options.make())) {
      Assert.assertFalse(db.get("foo").toCompletableFuture().get().isPresent());
    }
  }

  @Test
  public void testSingle() throws InterruptedException, ExecutionException {
    try (final DbStringWrapper db = new DbStringWrapper(Options.make())) {
      db.put("abc", "123")
          .thenCompose(voided -> db.get("abc").thenAccept(
              optGet -> Assert.assertEquals(optGet.orElseThrow(AssertionError::new), "123")))
          .toCompletableFuture().get();
    }

    try (DbStringWrapper db = new DbStringWrapper(Options.make())) {
      db.get("abc")
          .thenAccept(optGet -> Assert.assertEquals(optGet.orElseThrow(AssertionError::new), "123"))
          .toCompletableFuture().get();
    }
  }

  @Test
  public void testEmptyBatch() {
    try (DbStringWrapper db = new DbStringWrapper()) {
      final WriteBatch batch = db.db.createWriteBatch();
      db.db.write(batch);
    }
  }

  @Test
  public void testReadWrite() throws Exception {
    try (final DbStringWrapper db = new DbStringWrapper()) {
      // test from memtable
      db.put("foo", "v1").thenCompose(x -> db.get("foo"))
          .thenAccept(optFoo -> Assert.assertEquals(optFoo.get(), "v1"))
          .thenCompose(x -> db.put("bar", "v2")).thenCompose(x -> db.put("foo", "v3"))
          .thenCompose(x -> db.get("foo").thenAcceptBoth(db.get("bar"), (optFoo, optBar) -> {
            Assert.assertEquals(optFoo.get(), "v3");
            Assert.assertEquals(optBar.get(), "v2");
          })).toCompletableFuture().get();

      // test from level0
      db.put("bop", "v1").thenCompose(x -> db.flushMemTable()).thenCompose(x -> db.get("bop"))
          .thenAccept(optBop -> Assert.assertEquals(optBop.get(), "v1"))
          .thenCompose(x -> db.put("fiz", "v2")).thenCompose(x -> db.put("bop", "v3"))
          .thenCompose(x -> db.flushMemTable())
          .thenCompose(x -> db.get("bop").thenAcceptBoth(db.get("fiz"), (optBop, optFiz) -> {
            Assert.assertEquals(optBop.get(), "v3");
            Assert.assertEquals(optFiz.get(), "v2");
          })).toCompletableFuture().get();
    }
  }

  @Test
  public void testPutDeleteGet() throws Exception {
    try (final DbStringWrapper db = new DbStringWrapper()) {
      // test from memtable
      db.put("foo", "v1").thenCompose(x -> db.get("foo"))
          .thenAccept(optFoo -> Assert.assertEquals(optFoo.get(), "v1"))
          .thenCompose(x -> db.put("foo", "v2")).thenCompose(x -> db.get("foo"))
          .thenAccept(optFoo -> Assert.assertEquals(optFoo.get(), "v2"))
          .thenCompose(x -> db.delete("foo")).thenCompose(x -> db.get("foo"))
          .thenAccept(optFoo -> Assert.assertFalse(optFoo.isPresent())).toCompletableFuture().get();

      // test from level0
      db.put("bar", "v1").thenCompose(x -> db.flushMemTable()).thenCompose(x -> db.get("bar"))
          .thenAccept(optBar -> Assert.assertEquals(optBar.get(), "v1"))
          .thenCompose(x -> db.put("bar", "v2")).thenCompose(x -> db.get("bar"))
          .thenAccept(optBar -> Assert.assertEquals(optBar.get(), "v2"))
          .thenCompose(x -> db.flushMemTable()).thenCompose(x -> db.delete("bar"))
          .thenCompose(x -> db.flushMemTable()).thenCompose(x -> db.get("bar"))
          .thenAccept(optBar -> Assert.assertFalse(optBar.isPresent())).toCompletableFuture().get();
    }
  }

  static byte[] toByteArray(final String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  static ByteBuffer toBuf(final String s) {
    return ByteBuffer.wrap(toByteArray(s));
  }

  static String fromByteArray(final byte[] arr) {
    return new String(arr, StandardCharsets.UTF_8);
  }

  static String fromBuf(final ByteBuffer buf) {
    return fromByteArray(ByteBuffers.toArray(buf));
  }

  private class DbStringWrapper implements AutoCloseable {
    private final Options options;
    private final DbImpl db;

    private DbStringWrapper() {
      this(Options.make());
    }

    private DbStringWrapper(final Options givenOptions) {
      this.options = Options.copy(givenOptions).verifyChecksums(true).createIfMissing(true);
      try {
        this.db = DbImpl.newDbImpl(this.options, getHandle()).toCompletableFuture().get();
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      } catch (final ExecutionException e) {
        Throwables.propagate(e.getCause());
        throw new Error();
      }
    }

    public CompletionStage<String> getConfident(final String key) {
      return get(key).thenApply(Optional::get);
    }

    public CompletionStage<Optional<String>> get(final String key) {
      return get(key, ReadOptions.make());
    }

    public CompletionStage<Optional<String>> get(final String key, final Snapshot snapshot) {
      return get(key, ReadOptions.make().snapshot(snapshot));
    }

    public CompletionStage<Optional<String>> get(final String key, final ReadOptions readOptions) {
      return db.get(toBuf(key), readOptions)
          .thenApply(optValue -> optValue.map(DbImplTest2::fromBuf));
    }

    public CompletionStage<?> put(final String key, final String value) {
      return put(key, value, WriteOptions.make());
    }

    public CompletionStage<Snapshot> put(final String key, final String val,
        final WriteOptions writeOptions) {
      return db.put(toBuf(key), toBuf(val), writeOptions);
    }

    public CompletionStage<?> delete(final String key) {
      return delete(key, WriteOptions.make());
    }

    public CompletionStage<?> delete(final String key, final WriteOptions writeOptions) {
      return db.delete(toBuf(key), writeOptions);
    }

    public Snapshot getSnapshot() {
      return db.getSnapshot();
    }

    @Override
    public void close() {
      try {
        db.asyncClose().toCompletableFuture().get();
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      } catch (final ExecutionException e) {
        Throwables.propagate(e.getCause());
      }
    }

    public CompletionStage<?> flushMemTable() {
      return db.flushMemTable();
    }

    public int numberOfFilesInLevel(final int level) {
      return db.numberOfFilesInLevel(level);
    }

    public int totalTableFiles() {
      int result = 0;
      for (int level = 0; level < NUM_LEVELS; level++) {
        result += db.numberOfFilesInLevel(level);
      }
      return result;
    }

    public long getMaxNextLevelOverlappingBytes() {
      return db.getMaxNextLevelOverlappingBytes();
    }

  }

  public static class FileDbImplTest extends DbImplTest2 implements FileEnvTestProvider {
  }

}
