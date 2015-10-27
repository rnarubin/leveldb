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

import org.testng.Assert;
import org.testng.annotations.Test;

import com.cleversafe.leveldb.table.TestUtils;
import com.google.common.collect.Ordering;

public class MemTableIteratorTest {

  @Test
  public void testEmpty() {
    test(Collections.emptyList());
  }

  @Test
  public void testSingle() {
    test(Collections.singletonList(TestUtils.createInternalEntry("abc", "123", 1)));
  }

  @Test
  public void testMany() {
    test(Arrays.asList(TestUtils.createInternalEntry("abc", "123", 1),
        TestUtils.createInternalEntry("def", "456", 2),
        TestUtils.createInternalEntry("gg", "vg", 3),
        TestUtils.createInternalEntry("hijk", "44444", 4),
        TestUtils.createInternalEntry("lmnop", "q", 5)));
  }

  private void test(final List<Entry<InternalKey, ByteBuffer>> entries) {
    final MemTable memtable = new MemTable(TestUtils.keyComparator);

    for (final Entry<InternalKey, ByteBuffer> entry : entries) {
      memtable.add(entry.getKey(), entry.getValue());
    }

    Assert.assertTrue(
        Ordering.from(Comparator.comparing(Entry<InternalKey, ?>::getKey, TestUtils.keyComparator))
            .isOrdered(entries));
    TestUtils.testInternalKeyIterator(memtable.iterator(), entries, Runnable::run);
  }

}


