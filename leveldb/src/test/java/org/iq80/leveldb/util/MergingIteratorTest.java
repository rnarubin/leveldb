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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.iq80.leveldb.SeekingAsynchronousIterator;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.InternalKeyComparator;
import org.iq80.leveldb.impl.TransientInternalKey;
import org.iq80.leveldb.impl.ValueType;
import org.iq80.leveldb.table.BytewiseComparator;
import org.iq80.leveldb.table.TestHelper;
import org.iq80.leveldb.util.Iterators.AsyncWrappedSeekingIterator;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;

public class MergingIteratorTest {
  private static final Comparator<InternalKey> comparator =
      new InternalKeyComparator(new BytewiseComparator());

  @Test
  public void testEmpty() {
    iterTest(Collections.emptyList());
  }

  @Test
  public void testSingle() {
    iterTest(Collections.singletonList(Stream.of(1, 2, 3, 4, 5, 6, 7)
        .map(i -> TestHelper.createInternalEntry("" + i, "" + i, i)).collect(Collectors.toList())));
  }

  @Test
  public void testMultiple() {
    final int[][] data =
        {{'f', 'i', 'j', 's', 't'}, {'c', 'e', 'r', 'v'}, {'d'}, {'b', 'k', 'p', 'x', 'z'}, {}};
    iterTest(Arrays.stream(data).map(Arrays::stream)
        .map(stream -> stream.mapToObj(i -> new byte[] {(byte) i})
            .map(b -> Maps.<InternalKey, ByteBuffer>immutableEntry(
                new TransientInternalKey(ByteBuffer.wrap(b), b[0], ValueType.VALUE),
                ByteBuffer.wrap(b)))
            .collect(Collectors.toList()))
        .collect(Collectors.toList()));
  }

  private void iterTest(final List<List<Entry<InternalKey, ByteBuffer>>> input) {
    final SeekingAsynchronousIterator<InternalKey, ByteBuffer> iter =
        MergingIterator.newMergingIterator(input.stream()
            .map(list -> new AsyncWrappedSeekingIterator<>(
                Iterators.reverseSeekingIterator(list, comparator)))
            .collect(Collectors.toList()), comparator);
    final List<Entry<InternalKey, ByteBuffer>> entries =
        input.stream().reduce(new ArrayList<>(), (x, y) -> {
          x.addAll(y);
          return x;
        });
    entries.sort((o1, o2) -> comparator.compare(o1.getKey(), o2.getKey()));

    TestHelper.testInternalKeyIterator(iter, entries, r -> r.run());
  }
}


