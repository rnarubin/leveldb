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
package org.iq80.leveldb.impl;

import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;

import org.iq80.leveldb.MemoryManager;
import org.iq80.leveldb.table.UserComparator;

import static org.iq80.leveldb.impl.SequenceNumber.MAX_SEQUENCE_NUMBER;

public class InternalUserComparator
        implements UserComparator
{
    private final InternalKeyComparator internalKeyComparator;
    private final MemoryManager memory;

    public InternalUserComparator(InternalKeyComparator internalKeyComparator, MemoryManager memory)
    {
        this.internalKeyComparator = internalKeyComparator;
        this.memory = memory;
    }

    @Override
    public int compare(ByteBuffer left, ByteBuffer right)
    {
        return internalKeyComparator.compare(new InternalKey(left), new InternalKey(right));
    }

    @Override
    public String name()
    {
        return internalKeyComparator.name();
    }

    @Override
    public ByteBuffer findShortestSeparator(
            ByteBuffer start,
            ByteBuffer limit)
    {
        // Attempt to shorten the user portion of the key
        ByteBuffer startUserKey = InternalKey.getUserKey(start);
        ByteBuffer limitUserKey = InternalKey.getUserKey(limit);
        //Slice startUserKey = new InternalKey(start).getUserKey();
        //Slice limitUserKey = new InternalKey(limit).getUserKey();

        ByteBuffer shortestSeparator = internalKeyComparator.getUserComparator().findShortestSeparator(startUserKey, limitUserKey);

        if (internalKeyComparator.getUserComparator().compare(startUserKey, shortestSeparator) < 0) {
            // User key has become larger.  Tack on the earliest possible
            // number to the shortened user key.
            InternalKey newInternalKey = new InternalKey(shortestSeparator, MAX_SEQUENCE_NUMBER, ValueType.VALUE, memory);
            //TODO
            Preconditions.checkState(compare(start, newInternalKey.encode()) < 0); // todo
            Preconditions.checkState(compare(newInternalKey.encode(), limit) < 0); // todo

            //TODO refactor this comparator maybe
            return newInternalKey.encode();
        }

        return start;
    }

    @Override
    public ByteBuffer findShortSuccessor(ByteBuffer key)
    {
        ByteBuffer userKey = new InternalKey(key).getUserKey();
        ByteBuffer shortSuccessor = internalKeyComparator.getUserComparator().findShortSuccessor(userKey);

        if (internalKeyComparator.getUserComparator().compare(userKey, shortSuccessor) < 0) {
            // User key has become larger.  Tack on the earliest possible
            // number to the shortened user key.
            InternalKey newInternalKey = new InternalKey(shortSuccessor, MAX_SEQUENCE_NUMBER, ValueType.VALUE, memory);
            Preconditions.checkState(compare(key, newInternalKey.encode()) < 0); // todo

            return newInternalKey.encode();
        }

        return key;
    }
}
