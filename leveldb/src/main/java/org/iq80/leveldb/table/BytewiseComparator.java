/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.iq80.leveldb.table;

import java.nio.ByteBuffer;

import org.iq80.leveldb.DBBufferComparator;
import org.iq80.leveldb.util.ByteBuffers;

public class BytewiseComparator
        implements DBBufferComparator
{
    @Override
    public String name()
    {
        return "leveldb.BytewiseComparator";
    }

    @Override
    public int compare(ByteBuffer sliceA, ByteBuffer sliceB)
    {
        return ByteBuffers.compare(sliceA, sliceB);
    }

    @Override
    public boolean findShortestSeparator(ByteBuffer start, ByteBuffer limit)
    {
        // Find length of common prefix
        int sharedBytes = ByteBuffers.calculateSharedBytes(start, limit);

        // Do not shorten if one string is a prefix of the other
        if (sharedBytes < Math.min(start.remaining(), limit.remaining())) {
            // if we can add one to the last shared byte without overflow and the two keys differ by more than
            // one increment at this location.
            int startShared = start.position() + sharedBytes;
            int lastSharedByte = ByteBuffers.getUnsignedByte(start, startShared);
            if (lastSharedByte < 0xff
                    && lastSharedByte + 1 < ByteBuffers.getUnsignedByte(limit, limit.position() + sharedBytes)) {
                start.put(startShared, (byte) (lastSharedByte + 1));
                start.limit(startShared + 1);

                assert (compare(start, limit) < 0) : "start must be less than limit";
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean findShortSuccessor(ByteBuffer key)
    {
        // Find first character that can be incremented
        for (int i = 0; i < key.remaining(); i++) {
            int pos = key.position() + i;
            int b = ByteBuffers.getUnsignedByte(key, pos);
            if (b != 0xff) {
                key.put(pos, (byte) (b + 1));
                key.limit(pos + 1);
                return true;
            }
        }
        // key is a run of 0xffs.  Leave it alone.
        return false;
    }
}
