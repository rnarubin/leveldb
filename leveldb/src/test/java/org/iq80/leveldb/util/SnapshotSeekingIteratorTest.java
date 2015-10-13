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

package org.iq80.leveldb.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.iq80.leveldb.DBBufferComparator;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.InternalKeyComparator;
import org.iq80.leveldb.impl.SnapshotImpl;
import org.iq80.leveldb.impl.SnapshotSeekingIterator;
import org.iq80.leveldb.impl.TransientInternalKey;
import org.iq80.leveldb.impl.ValueType;
import org.iq80.leveldb.table.BytewiseComparator;
import org.iq80.leveldb.table.TestHelper;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;

public class SnapshotSeekingIteratorTest {
  private static final DBBufferComparator userComparator = new BytewiseComparator();
  private static final Comparator<InternalKey> keyComparator =
      new InternalKeyComparator(userComparator);
  private static final Comparator<Entry<InternalKey, ?>> entryComparator =
      (o1, o2) -> keyComparator.compare(o1.getKey(), o2.getKey());

  @Test
  public void testEmpty() {
    test(0, Collections.emptyList());
  }

  @Test
  public void testAllDeleted() {
    testArr(0, new int[][] {{'a', 0, 0}, {'a', 0, 1}, {'b', 0, 0}, {'b', 0, 1}, {'c', 0, 0}});
  }

  @Test
  public void testAllOverSequence() {
    testArr(1, new int[][] {{'a', 2, 1}, {'b', 2, 1}, {'c', 2, 1}});
  }

  @Test
  public void testAllIncluded() {
    testArr(17, new int[][] {{'3', 0, 1}, {'5', 0, 1}, {'7', 0, 1}, {'9', 0, 1}});
  }

  @Test(dataProvider = "variedData")
  public void testVaried(final long sequence, final int[][] data) {
    testArr(sequence, data);
  }

  @DataProvider
  public Object[][] variedData() {
    return new Object[][] {

        new Object[] {5,
            new int[][] {{'a', 6, 1}, {'a', 5, 1}, {'a', 4, 0}, {'a', 3, 1}, {'b', 6, 1},
                {'b', 6, 0}, {'b', 5, 0}, {'b', 4, 1}, {'b', 3, 1}, {'c', 4, 1}, {'d', 4, 0},
                {'d', 3, 1}, {'e', 6, 1}, {'f', 3, 1}, {'g', 7, 1}, {'h', 3, 1}}},

        new Object[] {3, new int[][] {{'a', 6, 1}, {'b', 7, 1}, {'c', 83, 1}, {'c', 52, 1},
            {'c', 2, 1}, {'c', 1, 1}, {'d', 12, 1}, {'e', 32, 1}, {'f', 18, 1}}},

        new Object[] {5,
            new int[][] {{'a', 6, 1}, {'a', 5, 1}, {'a', 4, 0}, {'a', 3, 1}, {'b', 6, 1},
                {'b', 6, 0}, {'b', 5, 0}, {'b', 4, 1}, {'b', 3, 1}, {'c', 4, 1}, {'d', 0, 1},
                {'m', 0, 0}, {'n', 0, 1}, {'o', 0, 0}, {'p', 12, 1}, {'v', 4, 0}, {'v', 3, 1},
                {'w', 6, 1}, {'x', 3, 1}, {'v', 7, 1}, {'v', 3, 0}}}

    };
  }

  /*
   * {key, seq, isValue}
   */
  private void testArr(final long sequence, final int[][] data) {
    final AtomicInteger disambiguator = new AtomicInteger(0);
    test(sequence,
        Arrays.stream(data)
            .map(x -> Maps.<InternalKey, ByteBuffer>immutableEntry(
                new TransientInternalKey(ByteBuffer.wrap(new byte[] {(byte) x[0]}), x[1],
                    x[2] == 0 ? ValueType.DELETION : ValueType.VALUE),
                ByteBuffer.wrap(new byte[] {(byte) x[0], (byte) x[1], (byte) x[2],
                    (byte) disambiguator.getAndIncrement()})))
            .collect(Collectors.toList()));
  }

  private void test(final long sequence, final List<Entry<InternalKey, ByteBuffer>> allEntries) {
    final List<Entry<ByteBuffer, ByteBuffer>> expectedEntries = allEntries.stream()
        .filter(entry -> entry.getKey().getSequenceNumber() <= sequence).sorted(entryComparator)
        .<List<Entry<InternalKey, ByteBuffer>>>reduce(new ArrayList<>(), (list, entry) -> {
          if (list.isEmpty() || !list.get(list.size() - 1).getKey().getUserKey()
              .equals(entry.getKey().getUserKey())) {
            list.add(entry);
          }
          return list;
        } , (x, y) -> {
          x.addAll(y);
          return x;
        }).stream().filter(entry -> entry.getKey().getValueType() == ValueType.VALUE)
        .map(entry -> Maps.immutableEntry(entry.getKey().getUserKey(), entry.getValue()))
        .collect(Collectors.toList());

    final SnapshotSeekingIterator iter = new SnapshotSeekingIterator(
        Iterators.async(Iterators.reverseSeekingIterator(allEntries, keyComparator)),
        new SnapshotImpl(sequence, null), userComparator, Runnable::run);

    TestHelper.testBufferIterator(iter, expectedEntries, Runnable::run);
  }
}


