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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Spliterator;
import java.util.concurrent.CompletionStage;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.cleversafe.leveldb.impl.FileMetaData;
import com.cleversafe.leveldb.impl.InternalKey;
import com.cleversafe.leveldb.impl.LookupKey;
import com.cleversafe.leveldb.impl.LookupResult;
import com.cleversafe.leveldb.impl.TableCache;
import com.cleversafe.leveldb.impl.TransientInternalKey;
import com.cleversafe.leveldb.impl.ValueType;
import com.cleversafe.leveldb.impl.Version;
import com.cleversafe.leveldb.table.TestUtils;
import com.cleversafe.leveldb.util.ByteBuffers;
import com.cleversafe.leveldb.util.CompletableFutures;
import com.cleversafe.leveldb.util.EnvDependentTest;
import com.cleversafe.leveldb.util.FileEnvTestProvider;
import com.google.common.collect.Maps;

public abstract class VersionTest extends EnvDependentTest {

  @Test
  public void testOverlapAssertion() {
    final FileMetaData[] nonOverlappingLevel = new FileMetaData[] {
        new FileMetaData(0, 0, TestUtils.createInternalKey("a", 0),
            TestUtils.createInternalKey("b", 0)),
        new FileMetaData(0, 0, TestUtils.createInternalKey("c", 0),
            TestUtils.createInternalKey("d", 0))};
    final FileMetaData[] overlappingLevel = new FileMetaData[] {
        new FileMetaData(0, 0, TestUtils.createInternalKey("a", 0),
            TestUtils.createInternalKey("c", 0)),
        new FileMetaData(0, 0, TestUtils.createInternalKey("b", 0),
            TestUtils.createInternalKey("d", 0))};

    for (int i = 0; i < NUM_LEVELS; i++) {
      final FileMetaData[][] files = new FileMetaData[NUM_LEVELS][];
      files[0] = overlappingLevel; // level 0 may always overlap
      for (int j = 1; j < files.length; j++) {
        files[j] = nonOverlappingLevel;
      }
      files[i] = overlappingLevel;

      final Version v = new Version(files, 0, 0, null, TestUtils.keyComparator);

      if (i == 0) {
        Assert.assertTrue(v.assertNoOverlappingFiles());
      } else {
        try {
          v.assertNoOverlappingFiles();
          Assert.fail("expected an exception");
        } catch (final Exception expected) {
        }
      }
    }
  }

  @Test
  public void testOverlappingIntputs() {
    // no overlap
    {
      final Version v = new Version(genMetaData(new String[][][] {{{"a", "b"}, {"x", "y"}}}), 0, 0,
          null, TestUtils.keyComparator);
      Assert.assertTrue(v.getOverlappingInputs(0, TestUtils.createInternalKey("c", 0),
          TestUtils.createInternalKey("w", 0)).isEmpty());
    }

    // exact overlap
    {
      final FileMetaData[][] files = genMetaData(
          new String[][][] {{{"a", "b"}, {"c", "f"}, {"d", "f"}, {"h", "i"}, {"e", "e"}}});
      final Version v = new Version(files, 0, 0, null, TestUtils.keyComparator);
      Assert.assertEquals(Arrays.asList(files[0][3]), v.getOverlappingInputs(0,
          TestUtils.createInternalKey("h", 0), TestUtils.createInternalKey("i", 0)));
    }

    // lower bound overlap
    {
      final FileMetaData[][] files = genMetaData(
          new String[][][] {{{"a", "b"}, {"c", "f"}, {"d", "f"}, {"h", "i"}, {"e", "e"}}});
      final Version v = new Version(files, 0, 0, null, TestUtils.keyComparator);
      Assert.assertEquals(Arrays.asList(files[0][1], files[0][2], files[0][4]),
          v.getOverlappingInputs(0, TestUtils.createInternalKey("e", 0),
              TestUtils.createInternalKey("g", 0)));
    }

    // upper bound overlap
    {
      final FileMetaData[][] files = genMetaData(
          new String[][][] {{{"a", "a"}, {"c", "f"}, {"d", "f"}, {"h", "i"}, {"g", "g"}}});
      final Version v = new Version(files, 0, 0, null, TestUtils.keyComparator);
      Assert.assertEquals(Arrays.asList(files[0][1], files[0][2], files[0][4]),
          v.getOverlappingInputs(0, TestUtils.createInternalKey("e", 0),
              TestUtils.createInternalKey("g", 0)));
    }

    // contained inner, outer
    {
      final FileMetaData[][] files = genMetaData(new String[][][] {
          {{"a", "b"}, {"c", "d"}, {"e", "n"}, {"g", "k"}, {"w", "z"}, {"f", "m"}, {"q", "x"}}});
      final Version v = new Version(files, 0, 0, null, TestUtils.keyComparator);
      Assert.assertEquals(Arrays.asList(files[0][2], files[0][3], files[0][5]),
          v.getOverlappingInputs(0, TestUtils.createInternalKey("f", 0),
              TestUtils.createInternalKey("m", 0)));
    }

    // cascading
    {
      final FileMetaData[][] files = genMetaData(new String[][][] {
          {{"a", "b"}, {"c", "d"}, {"e", "i"}, {"g", "k"}, {"w", "z"}, {"h", "m"}, {"d", "f"}}});
      final Version v = new Version(files, 0, 0, null, TestUtils.keyComparator);
      Assert.assertEquals(
          Arrays.asList(files[0][1], files[0][2], files[0][3], files[0][5], files[0][6]),
          v.getOverlappingInputs(0, TestUtils.createInternalKey("f", 0),
              TestUtils.createInternalKey("g", 0)));
    }

  }

  private FileMetaData[][] genMetaData(final String[][][] levels) {
    return Arrays.stream(levels)
        .map(files -> Arrays.stream(files)
            .map(bounds -> new FileMetaData(0, 0, TestUtils.createInternalKey(bounds[0], 0),
                TestUtils.createInternalKey(bounds[1], 0)))
            .toArray(FileMetaData[]::new))
        .toArray(FileMetaData[][]::new);
  }

  @Test
  public void testGets() throws Exception {
    //@formatter:off
    // \0 indicates deleted key
    final String[][] level0 =
        // 0      1      2      3      4      5      6    7      8    9    10   11
        {{"a", "\0c"}, {"b", "c", "\0d"}, {"\0c", "\0o", "w"}, {"d", "g", "o", "t"}};
    final String[][] level1 =
        //12   13    14   15     16     17   18   19   20   21       22     23   24   25     26
        {{"a", "b", "c", "d"}, {"e", "\0f", "g", "h", "i", "j"}, {"\0k"}, {"l", "m", "n"}, {"o"}};
    final String[][] level2 = {};
    //                           27       28     29
    final String[][] level3 = {{"x"}, {"\0y"}, {"z"}};
    //                           30   31     32   33   34     35   36   37    38
    final String[][] level4 = {{"f", "h"}, {"i", "j", "k"}, {"p", "q", "r", "\0s"}};

    final LongSupplier seqGen = TestUtils.counter(0);
    final List<List<List<Entry<InternalKey, ByteBuffer>>>> levelEntries =
    Stream.of(level0, level1, level2, level3, level4)
    .map(level -> Arrays.stream(level)
        .map(table -> Arrays.stream(table)
            .map(key -> {
              final long seq = seqGen.getAsLong();
              return Maps.<InternalKey, ByteBuffer>immutableEntry(
                  new TransientInternalKey(
                    TestUtils.toBuf(key.substring(key.indexOf('\0') + 1)),
                    seq,
                    key.charAt(0) == '\0' ? ValueType.DELETION : ValueType.VALUE),
                key.charAt(0) == '\0' ? ByteBuffers.EMPTY_BUFFER : TestUtils.toBuf("seq:"+seq+":value:" + key));
              })
            .collect(Collectors.toList()))
        .collect(Collectors.toList()))
    .collect(Collectors.toList());
    //@formatter:on
    final long seq = seqGen.getAsLong();

    final List<List<Entry<InternalKey, ByteBuffer>>> tableEntries =
        levelEntries.stream().flatMap(List::stream).collect(Collectors.toList());
    final Entry<TableCache, FileMetaData[]> tables = TestUtils.generateTableCache(getEnv(),
        getHandle(), tableEntries, TestUtils.counter(0), 30, 16, 100);

    final Spliterator<FileMetaData> tableFiles = Arrays.spliterator(tables.getValue());
    final List<List<FileMetaData>> levels = new ArrayList<>();
    for (final List<?> singleLevelEntries : levelEntries) {
      final List<FileMetaData> singleLevelFiles = new ArrayList<>();
      for (int i = 0; i < singleLevelEntries.size(); i++) {
        Assert.assertTrue(tableFiles.tryAdvance(file -> singleLevelFiles.add(file)));
      }
      levels.add(singleLevelFiles);
    }
    Assert.assertFalse(tableFiles.tryAdvance(x -> {
      return;
    }));

    final FileMetaData[][] levelFiles = levels.stream()
        .map(list -> list.stream().toArray(FileMetaData[]::new)).toArray(FileMetaData[][]::new);

    final TableCache tableCache = tables.getKey();
    try (AutoCloseable c = TestUtils.autoCloseable(tableCache)) {
      final Version v = new Version(levelFiles, 0, 0, tableCache, TestUtils.keyComparator);
      v.assertNoOverlappingFiles();

      final Stream.Builder<CompletionStage<Void>> work = Stream.builder();
      for (final LookupResult expected : Arrays.asList(genResult("g", false, seq, 9),
          genResult("c", true, seq, 5), genResult("c", false, 3, 3), genResult("e", false, seq, 16),
          genResult("f", true, seq, 17), genResult("x", false, seq, 27),
          genResult("s", true, seq, 38))) {
        final LookupKey target = expected.getKey();
        work.add(v.get(target).thenAccept(actual -> assertLookupEquals(actual, expected)));
      }

      for (final LookupKey missingTarget : Arrays.asList(genLookup("v", seq), genLookup("d", 3),
          genLookup("q", 35))) {
        work.add(v.get(missingTarget).thenAccept(actual -> Assert.assertNull(actual)));
      }
      CompletableFutures.allOfVoid(work.build()).toCompletableFuture().get();
    }
  }

  private LookupKey genLookup(final String k, final long seq) {
    return new LookupKey(TestUtils.toBuf(k), seq);
  }

  private LookupResult genResult(final String k, final boolean deleted, final long lookupSequence,
      final long expectedSequence) {
    return deleted ? LookupResult.deleted(genLookup(k, lookupSequence))
        : LookupResult.ok(genLookup(k, lookupSequence),
            TestUtils.toBuf("seq:" + expectedSequence + ":value:" + k));
  }

  public static void assertLookupEquals(final LookupResult actual, final LookupResult expected) {
    if (actual == null || expected == null) {
      Assert.assertTrue(actual == expected);
    } else {
      Assert.assertEquals(actual.isDeleted(), expected.isDeleted());
      TestUtils.assertByteBufferEquals(actual.getKey().getUserKey(),
          expected.getKey().getUserKey());
      TestUtils.assertByteBufferEquals(actual.getValue(), expected.getValue());
    }
  }



  public static class FileVersionTest extends VersionTest implements FileEnvTestProvider {
  }
}


