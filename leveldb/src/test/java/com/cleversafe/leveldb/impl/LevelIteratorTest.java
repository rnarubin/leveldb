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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.cleversafe.leveldb.impl.Version.LevelIterator;
import com.cleversafe.leveldb.table.TestUtils;
import com.cleversafe.leveldb.util.EnvDependentTest;
import com.cleversafe.leveldb.util.FileEnvTestProvider;
import com.google.common.collect.Ordering;

public abstract class LevelIteratorTest extends EnvDependentTest {
  @Test
  public void testEmpty() {
    test(Collections.emptyList(), 4 << 10, 16);
  }

  @Test
  public void testSingle() {
    final List<List<Entry<InternalKey, ByteBuffer>>> tableEntries =
        Collections.singletonList(Arrays.asList(TestUtils.createInternalEntry("aaabbbccc", "v1", 0),
            TestUtils.createInternalEntry("aabbcc", "v2", 1),
            TestUtils.createInternalEntry("abc", "v3", 2),
            TestUtils.createInternalEntry("qwerty", "v4", 3),
            TestUtils.createInternalEntry("xxxyyyzzz", "v5", 4),
            TestUtils.createInternalEntry("xxyyzz", "v6", 5),
            TestUtils.createInternalEntry("xyz", "v7", 6)));

    for (int i = 1; i < tableEntries.get(0).size(); i++) {
      test(tableEntries, 20, i);
    }
  }

  @Test(dataProvider = "getData")
  public void testMany(final String[][] keys) {
    final AtomicInteger i = new AtomicInteger(0);
    final List<List<Entry<InternalKey, ByteBuffer>>> tableEntries =
        Stream.of(keys).map(Stream::of)
            .map(table -> table.map(k -> TestUtils.createInternalEntry(k, "v" + i.getAndIncrement(),
                i.getAndIncrement())))
            .map(stream -> stream.collect(Collectors.toList())).collect(Collectors.toList());
    test(tableEntries, 20, 16);
  }

  @DataProvider
  public Object[][] getData() {
    return new Object[][] {

        {new String[][] {{"abc", "def", "ghi"}, {"jjjkk", "jjjklm", "la", "llb", "lllllc"}, {"q"},
            {"xxxyyyzzz", "xxyyzz"}, {"zzzzz"}}}

    };
  }

  private void test(final List<List<Entry<InternalKey, ByteBuffer>>> tableEntries,
      final int blockSize, final int blockRestartInterval) {
    try {
      final Entry<TableCache, FileMetaData[]> tables = TestUtils.generateTableCache(getEnv(),
          getHandle(), tableEntries, TestUtils.counter(0), blockSize, blockRestartInterval, 100);

      final TableCache tableCache = tables.getKey();
      try (AutoCloseable c = TestUtils.autoCloseable(tableCache)) {
        final List<Entry<InternalKey, ByteBuffer>> expected =
            tableEntries.stream().flatMap(List::stream).collect(Collectors.toList());
        Assert.assertTrue(Ordering
            .from(Comparator.comparing(Entry<InternalKey, ?>::getKey, TestUtils.keyComparator))
            .isOrdered(expected));
        final LevelIterator iter =
            new LevelIterator(tableCache, tables.getValue(), TestUtils.keyComparator);

        TestUtils.testInternalKeyIterator(iter, expected);
      }
    } catch (final Exception e) {
      throw new AssertionError(e);
    }
  }

  public static class FileLevelIteratorTest extends LevelIteratorTest
      implements FileEnvTestProvider {
  }
}


