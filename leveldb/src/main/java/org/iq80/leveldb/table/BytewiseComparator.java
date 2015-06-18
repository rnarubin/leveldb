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
import org.iq80.leveldb.MemoryManager;
import org.iq80.leveldb.util.ByteBuffers;

public class BytewiseComparator
        implements DBBufferComparator
{
    private final MemoryManager memory;

    public BytewiseComparator(MemoryManager memory)
    {
        this.memory = memory;
    }
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
    public ByteBuffer findShortestSeparator(ByteBuffer start, ByteBuffer limit)
    {
        // Find length of common prefix
        int sharedBytes = ByteBuffers.calculateSharedBytes(start, limit);

        // Do not shorten if one string is a prefix of the other
        if (sharedBytes < Math.min(start.remaining(), limit.remaining())) {
            // if we can add one to the last shared byte without overflow and the two keys differ by more than
            // one increment at this location.
            int lastSharedByte = ByteBuffers.getUnsignedByte(start, sharedBytes);
            if (lastSharedByte < 0xff && lastSharedByte + 1 < ByteBuffers.getUnsignedByte(limit, sharedBytes)) {
                //Slice result = start.copySlice(0, sharedBytes + 1);
                //ByteBuffer result = memory.allocate(sharedBytes+1);
                ByteBuffer result = ByteBuffers.copy(start, sharedBytes+1, memory);
                result.put(sharedBytes, (byte)(lastSharedByte + 1));

                assert (compare(result, limit) < 0) : "start must be less than last limit";
                return result;
            }
        }
        return start;
    }

    @Override
    public ByteBuffer findShortSuccessor(ByteBuffer key)
    {
        // Find first character that can be incremented
        for (int i = 0; i < key.remaining(); i++) {
            int b = ByteBuffers.getUnsignedByte(key, i);
            if (b != 0xff) {
                //Slice result = key.copySlice(0, i + 1);
                ByteBuffer result = ByteBuffers.copy(key, i+1, memory);
                result.put(i, (byte)(b + 1));
                return result;
            }
        }
        // key is a run of 0xffs.  Leave it alone.
        return key;
    }
}
