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

package com.cleversafe.leveldb.util;

import static com.cleversafe.leveldb.table.TestUtils.keyComparator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.cleversafe.leveldb.impl.InternalKey;
import com.cleversafe.leveldb.impl.TransientInternalKey;
import com.cleversafe.leveldb.impl.ValueType;
import com.cleversafe.leveldb.table.TestUtils;
import com.cleversafe.leveldb.util.Iterators;
import com.cleversafe.leveldb.util.MergingIterator;
import com.cleversafe.leveldb.util.SeekingAsynchronousIterator;
import com.google.common.collect.Maps;

public class MergingIteratorTest {
  @Test
  public void testEmpty() {
    iterTest(Collections.emptyList());
  }

  @Test
  public void testSingle() {
    iterTest(Collections.singletonList(Stream.of(1, 2, 3, 4, 5, 6, 7)
        .map(i -> TestUtils.createInternalEntry("" + i, "" + i, i)).collect(Collectors.toList())));
  }

  @Test(dataProvider = "getData")
  public void testMultiple(final int[][] data) {
    iterTest(Arrays.stream(data).map(Arrays::stream)
        .map(stream -> stream.mapToObj(i -> new byte[] {(byte) i})
            .map(b -> Maps.<InternalKey, ByteBuffer>immutableEntry(
                new TransientInternalKey(ByteBuffer.wrap(b), b[0], ValueType.VALUE),
                ByteBuffer.wrap(b)))
            .collect(Collectors.toList()))
        .collect(Collectors.toList()));
  }

  @DataProvider
  public Object[][] getData() {
    return new Object[][] {
        {new int[][] {{'f', 'i', 'j', 's', 't'}, {'c', 'e', 'r', 'v'}, {'d'},
            {'b', 'k', 'p', 'x', 'z'}, {}, {'a', 'l', 'n'}, {'m', 'o'}}},
        {new int[][] {{'a', 'b', 'c'}, {'d', 'e', 'f'}, {'h', 'i', 'j'}}}};
  }

  private void iterTest(final List<List<Entry<InternalKey, ByteBuffer>>> input) {
    final SeekingAsynchronousIterator<InternalKey, ByteBuffer> iter =
        MergingIterator
            .newMergingIterator(
                input.stream()
                    .map(list -> Iterators
                        .async(Iterators.reverseSeekingIterator(list, keyComparator)))
                .collect(Collectors.toList()), keyComparator);
    final List<Entry<InternalKey, ByteBuffer>> entries =
        input.stream().reduce(new ArrayList<>(), (x, y) -> {
          x.addAll(y);
          return x;
        });
    entries.sort((o1, o2) -> keyComparator.compare(o1.getKey(), o2.getKey()));

    TestUtils.testInternalKeyIterator(iter, entries, Runnable::run);
  }
}

