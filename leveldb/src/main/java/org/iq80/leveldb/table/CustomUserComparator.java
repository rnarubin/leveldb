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

import org.iq80.leveldb.DBComparator;
import org.iq80.leveldb.util.ByteBuffers;

public class CustomUserComparator
        implements UserComparator
{
    private final DBComparator comparator;

    public CustomUserComparator(DBComparator comparator)
    {
        this.comparator = comparator;
    }

    @Override
    public String name()
    {
        return comparator.name();
    }

    @Override
    public ByteBuffer findShortestSeparator(ByteBuffer start, ByteBuffer limit)
    {
        return ByteBuffer.wrap(comparator.findShortestSeparator(ByteBuffers.toArray(start), ByteBuffers.toArray(limit)));
    }

    @Override
    public ByteBuffer findShortSuccessor(ByteBuffer key)
    {
        return ByteBuffer.wrap(comparator.findShortSuccessor(ByteBuffers.toArray(key)));
    }

    @Override
    public int compare(ByteBuffer o1, ByteBuffer o2)
    {
        return comparator.compare(ByteBuffers.toArray(o1), ByteBuffers.toArray(o2));
    }
}
