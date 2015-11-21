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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.cleversafe.leveldb.FileInfo;
import com.cleversafe.leveldb.table.TestUtils;
import com.cleversafe.leveldb.table.TestUtils.EqualableFileMetaData;
import com.cleversafe.leveldb.util.CompletableFutures;
import com.cleversafe.leveldb.util.EnvDependentTest;
import com.cleversafe.leveldb.util.FileEnvTestProvider;
import com.cleversafe.leveldb.util.Iterators;
import com.cleversafe.leveldb.util.SeekingAsynchronousIterator;
import com.google.common.collect.Maps;

public abstract class VersionSetTest extends EnvDependentTest {

  @Test
  public void testLoad() throws Exception {
    // creation from empty
    {
      final VersionSet vs =
          VersionSet.newVersionSet(getHandle(), null, TestUtils.keyComparator, getEnv())
              .toCompletableFuture().get();

      try (AutoCloseable c = TestUtils.autoCloseable(vs)) {
        Assert.assertTrue(
            getEnv().fileExists(FileInfo.current(getHandle())).toCompletableFuture().get());
        Assert.assertTrue(
            getEnv().fileExists(FileInfo.manifest(getHandle(), 1L)).toCompletableFuture().get());
        Assert.assertEquals(vs.getLogNumber(), 0L);
        Assert.assertEquals(vs.getManifestFileNumber(), 1L);
        Assert.assertEquals(vs.getAndIncrementNextFileNumber(), 2L);
        Assert.assertEquals(vs.getLastSequence(), 0L);
        Assert.assertEquals(vs.getAndAddLastSequence(17L), 0L);
        Assert.assertEquals(vs.getLastSequence(), 17L);
        Assert.assertFalse(vs.getLiveFiles().findAny().isPresent());

        vs.getAndIncrementNextFileNumber();
        vs.getAndIncrementNextFileNumber();
      }
    }

    final FileMetaData[][] addedFiles = new FileMetaData[][] {
        {new FileMetaData(4, 7, TestUtils.createInternalKey("f", 9),
            TestUtils.createInternalKey("j", 3)),
        new FileMetaData(10, 15, TestUtils.createInternalKey("r", 0),
            TestUtils.createInternalKey("z", 6))},
        {}, {new FileMetaData(12, 4, TestUtils.createInternalKey("g", 0),
            TestUtils.createInternalKey("j", 3))}};
    // recovery, no edit applied
    {
      final VersionSet vs =
          VersionSet.newVersionSet(getHandle(), null, TestUtils.keyComparator, getEnv())
              .toCompletableFuture().get();
      try (AutoCloseable c = TestUtils.autoCloseable(vs)) {
        Assert.assertEquals(vs.getLogNumber(), 0L);

        // incremented from recovery, changes weren't saved
        Assert.assertEquals(vs.getManifestFileNumber(), 2L);
        Assert.assertEquals(vs.getAndIncrementNextFileNumber(), 3L);

        // still 0 as change wasn't saved
        Assert.assertEquals(vs.getLastSequence(), 0L);
        Assert.assertEquals(vs.getAndAddLastSequence(3L), 0L);
        Assert.assertEquals(vs.getLastSequence(), 3L);
        Assert.assertFalse(vs.getLiveFiles().findAny().isPresent());

        vs.getAndIncrementNextFileNumber();
        vs.getAndIncrementNextFileNumber();


        // now apply edit for next load
        final VersionEdit edit = new VersionEdit();
        edit.addFiles(addedFiles);
        edit.setCompactPointer(2, TestUtils.createInternalKey("a", 9));
        edit.setLogNumber(5);
        vs.logAndApply(edit).toCompletableFuture().get();
      }
    }


    final FileMetaData[][] addedFiles2 = new FileMetaData[][] {
        {new FileMetaData(11, 7, TestUtils.createInternalKey("a", 9),
            TestUtils.createInternalKey("z", 3)),
        new FileMetaData(13, 15, TestUtils.createInternalKey("d", 0),
            TestUtils.createInternalKey("f", 6))},
        {new FileMetaData(14, 0, TestUtils.createInternalKey("x", 0),
            TestUtils.createInternalKey("y", 0))},
        {new FileMetaData(15, 4, TestUtils.createInternalKey("a", 0),
            TestUtils.createInternalKey("b", 3)),
            new FileMetaData(16, 0, TestUtils.createInternalKey("q", 0),
                TestUtils.createInternalKey("w", 0)),
            new FileMetaData(17, 0, TestUtils.createInternalKey("x", 0),
                TestUtils.createInternalKey("z", 1))}};
    final List<Entry<Integer, FileMetaData>> deletedFiles = Arrays
        .asList(Maps.immutableEntry(0, addedFiles[0][1]), Maps.immutableEntry(2, addedFiles[2][0]));

    // recovery, load edit
    {
      final VersionSet vs =
          VersionSet.newVersionSet(getHandle(), null, TestUtils.keyComparator, getEnv())
              .toCompletableFuture().get();
      try (AutoCloseable c = TestUtils.autoCloseable(vs)) {
        Assert.assertEquals(vs.getLogNumber(), 5L);
        Assert.assertEquals(vs.getManifestFileNumber(), 6L);
        Assert.assertEquals(vs.getAndIncrementNextFileNumber(), 7L);
        Assert.assertEquals(vs.getLastSequence(), 3L);
        Assert.assertEquals(vs.getAndAddLastSequence(7L), 3L);
        Assert.assertEquals(vs.getLastSequence(), 10L);
        final List<EqualableFileMetaData> expectedFiles = Arrays.stream(addedFiles)
            .flatMap(Arrays::stream).map(EqualableFileMetaData::new).collect(Collectors.toList());
        final List<EqualableFileMetaData> actualFiles =
            vs.getLiveFiles().map(EqualableFileMetaData::new).collect(Collectors.toList());
        Assert.assertTrue(expectedFiles.containsAll(actualFiles));
        Assert.assertTrue(actualFiles.containsAll(expectedFiles));

        {
          final VersionEdit edit = new VersionEdit();
          edit.addFiles(addedFiles2);
          for (final Entry<Integer, FileMetaData> deletedFile : deletedFiles) {
            edit.deleteFile(deletedFile.getKey(), deletedFile.getValue().getNumber());
          }
          vs.logAndApply(edit).toCompletableFuture().get();
        }


        {
          final VersionEdit edit = new VersionEdit();
          edit.setCompactPointer(0, TestUtils.createInternalKey("e", 0));
          edit.setCompactPointer(2, TestUtils.createInternalKey("f", 0));
          vs.logAndApply(edit).toCompletableFuture().get();
        }
      }
    }

    // recovery, load edit with deletes
    {
      final VersionSet vs =
          VersionSet.newVersionSet(getHandle(), null, TestUtils.keyComparator, getEnv())
              .toCompletableFuture().get();
      try (AutoCloseable c = TestUtils.autoCloseable(vs)) {
        Assert.assertEquals(vs.getLogNumber(), 5L);
        Assert.assertEquals(vs.getManifestFileNumber(), 8L);
        Assert.assertEquals(vs.getAndIncrementNextFileNumber(), 9L);
        Assert.assertEquals(vs.getLastSequence(), 10L);
        final List<EqualableFileMetaData> expectedFiles =
            Stream.concat(Arrays.stream(addedFiles), Arrays.stream(addedFiles2))
                .flatMap(
                    Arrays::stream)
            .map(EqualableFileMetaData::new)
            .filter(file -> !deletedFiles.stream()
                .map(entry -> new EqualableFileMetaData(entry.getValue())).anyMatch(file::equals))
            .collect(Collectors.toList());
        final List<EqualableFileMetaData> actualFiles =
            vs.getLiveFiles().map(EqualableFileMetaData::new).collect(Collectors.toList());
        Assert.assertTrue(expectedFiles.containsAll(actualFiles));
        Assert.assertTrue(actualFiles.containsAll(expectedFiles));
      }
    }
  }

  @Test
  public void testWeakReferenceGet() throws Exception {
    final FileMetaData file1 = new FileMetaData(1, 0, TestUtils.createInternalKey("a", 0),
        TestUtils.createInternalKey("c", 0));

    final Semaphore blocker = new Semaphore(0);
    final TableCache delayedTableCache =
        new TableCache(getHandle(), 100, TestUtils.keyComparator, getEnv(), true, null, null) {
          @Override
          public CompletionStage<SeekingAsynchronousIterator<InternalKey, ByteBuffer>> tableIterator(
              final FileMetaData file) {
            if (file.getNumber() == file1.getNumber()) {
              blocker.acquireUninterruptibly();
            }
            return CompletableFuture.completedFuture(Iterators.emptySeekingAsyncIterator());
          }
        };
    final VersionSet vs =
        VersionSet.newVersionSet(getHandle(), delayedTableCache, TestUtils.keyComparator, getEnv())
            .toCompletableFuture().get();
    try (AutoCloseable c = TestUtils.autoCloseable(vs)) {
      {
        final VersionEdit edit = new VersionEdit();
        edit.addFile(1, file1);
        vs.logAndApply(edit).toCompletableFuture().get();
      }

      TestUtils.assertFileEquals(vs.getLiveFiles().findFirst().orElseThrow(AssertionError::new),
          file1);

      // lookup will block until semaphore released
      final CompletableFuture<LookupResult> lookup = new CompletableFuture<>();
      new Thread(() -> CompletableFutures.compose(
          vs.get(new LookupKey(ByteBuffer.wrap("b".getBytes(StandardCharsets.UTF_8)), 0)), lookup))
              .start();

      {
        final VersionEdit edit = new VersionEdit();
        edit.deleteFile(1, file1.getNumber());
        vs.logAndApply(edit).toCompletableFuture().get();
      }

      System.gc();
      TestUtils.assertFileEquals(vs.getLiveFiles().findFirst().orElseThrow(AssertionError::new),
          file1);
      blocker.release();
      lookup.get();
      System.gc();
      Assert.assertFalse(vs.getLiveFiles().findFirst().isPresent());
    }
  }

  public static class FileVersionSetTest extends VersionSetTest implements FileEnvTestProvider {
  }

}


