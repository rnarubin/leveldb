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
package com.cleversafe.leveldb.table;

import java.nio.ByteBuffer;

import com.cleversafe.leveldb.DBComparator;
import com.cleversafe.leveldb.util.ByteBuffers;

public class BytewiseComparator implements DBComparator {
  private BytewiseComparator() {}

  private static final BytewiseComparator INSTANCE = new BytewiseComparator();

  public static BytewiseComparator instance() {
    return INSTANCE;
  }

  @Override
  public String name() {
    return "leveldb.BytewiseComparator";
  }

  @Override
  public int compare(final ByteBuffer sliceA, final ByteBuffer sliceB) {
    return ByteBuffers.compare(sliceA, sliceB);
  }

  @Override
  public boolean findShortestSeparator(final ByteBuffer start, final ByteBuffer limit) {
    // Find length of common prefix
    final int sharedBytes = ByteBuffers.calculateSharedBytes(start, limit);

    // Do not shorten if one string is a prefix of the other
    if (sharedBytes < Math.min(start.remaining(), limit.remaining())) {
      // if we can add one to the last shared byte without overflow and the two keys differ by more
      // than one increment at this location.
      final int startShared = start.position() + sharedBytes;
      final int lastSharedByte = ByteBuffers.getUnsignedByte(start, startShared);
      if (lastSharedByte < 0xff && lastSharedByte + 1 < ByteBuffers.getUnsignedByte(limit,
          limit.position() + sharedBytes)) {
        start.put(startShared, (byte) (lastSharedByte + 1));
        start.limit(startShared + 1);

        assert (compare(start, limit) < 0) : "start must be less than limit";
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean findShortSuccessor(final ByteBuffer key) {
    // Find first character that can be incremented
    for (int i = 0; i < key.remaining(); i++) {
      final int pos = key.position() + i;
      final int b = ByteBuffers.getUnsignedByte(key, pos);
      if (b != 0xff) {
        key.put(pos, (byte) (b + 1));
        key.limit(pos + 1);
        return true;
      }
    }
    // key is a run of 0xffs. Leave it alone.
    return false;
  }
}
