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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.iq80.leveldb.DBException;
import org.iq80.leveldb.MemoryManager;
import org.iq80.leveldb.util.Closeables;
import org.iq80.leveldb.util.InternalIterator;
import org.iq80.leveldb.util.ReferenceCounted;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public final class MemTable
        extends ReferenceCounted<MemTable>
        implements SeekingIterable<InternalKey, ByteBuffer>
{
    private final ConcurrentSkipListMap<InternalKey, ByteBuffer> table;
    private final AtomicLong approximateMemoryUsage = new AtomicLong();
    private final Comparator<Entry<InternalKey, ByteBuffer>> iteratorComparator;
    private final List<AutoCloseable> cleanup;

    public MemTable(final InternalKeyComparator internalKeyComparator, boolean bufferCleaner, MemoryManager memory)
    {
        this.table = new ConcurrentSkipListMap<InternalKey, ByteBuffer>(internalKeyComparator);
        this.iteratorComparator = new Comparator<Entry<InternalKey, ByteBuffer>>()
        {
            public int compare(Entry<InternalKey, ByteBuffer> o1, Entry<InternalKey, ByteBuffer> o2)
            {
                return internalKeyComparator.compare(o1.getKey(), o2.getKey());
            }
        };

        this.cleanup = new ArrayList<>();
        if (bufferCleaner) {
            registerCleanup(new MemTableCleaner(this, memory));
        }
    }

    public void registerCleanup(Closeable cleaner)
    {
        this.cleanup.add(cleaner);
    }

    public void clear()
    {
        table.clear();
    }

    public boolean isEmpty()
    {
        return table.isEmpty();
    }

    public long approximateMemoryUsage()
    {
        return approximateMemoryUsage.get();
    }

    protected long getAndAddApproximateMemoryUsage(long delta)
    {
        return approximateMemoryUsage.getAndAdd(delta);
    }

    public void add(InternalKey internalKey, ByteBuffer value)
    {
        table.put(internalKey, value);
    }

    public LookupResult get(LookupKey key)
    {
        Preconditions.checkNotNull(key, "key is null");

        InternalKey internalKey = key.getInternalKey();
        Entry<InternalKey, ByteBuffer> entry = table.ceilingEntry(internalKey);
        if (entry == null) {
            return null;
        }

        InternalKey entryKey = entry.getKey();
        if (entryKey.getUserKey().equals(key.getUserKey())) {
            if (entryKey.getValueType() == ValueType.DELETION) {
                return LookupResult.deleted(key);
            }
            else {
                return LookupResult.ok(key, entry.getValue(), false);
            }
        }
        return null;
    }

    public Iterable<Entry<InternalKey, ByteBuffer>> simpleIterable()
    {
        return table.entrySet();
    }

    @Override
    public MemTableIterator iterator()
    {
        return new MemTableIterator();
    }

    @Override
    protected MemTable getThis()
    {
        return this;
    }

    @Override
    protected void dispose()
    {
        try {
            Closeables.closeIO(cleanup);
        }
        catch (IOException e) {
            throw new DBException(e);
        }
    }

    public final class MemTableIterator
            implements InternalIterator
    {
        private ReversePeekingIterator<Entry<InternalKey, ByteBuffer>> iterator;
        private final List<Entry<InternalKey, ByteBuffer>> entryList;

        public MemTableIterator()
        {
            retain();
            entryList = Lists.newArrayList(simpleIterable());
            seekToFirst();
        }

        @Override
        public boolean hasNext()
        {
            return iterator.hasNext();
        }

        @Override
        public void seekToFirst()
        {
            makeIteratorAtIndex(0);
        }

        @Override
        public void seek(InternalKey targetKey)
        {
            int index = Collections.binarySearch(entryList, Maps.immutableEntry(targetKey, (ByteBuffer) null),
                    iteratorComparator);
            if (index < 0) {
            /*
             * from Collections.binarySearch:
             * index: the index of the search key, if it is contained in the list; otherwise, (-(insertion point) - 1).
             * The insertion point is defined as the point at which the key would be inserted into the list:
             * the index of the first element greater than the key, or list.size() if all elements in the list are
             * less than the specified key
             */
                index = -(index + 1);
            }
            makeIteratorAtIndex(index);
        }

        @Override
        public Entry<InternalKey, ByteBuffer> peek()
        {
            return iterator.peek();
        }

        @Override
        public Entry<InternalKey, ByteBuffer> next()
        {
            return iterator.next();
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void seekToEnd()
        {
            makeIteratorAtIndex(this.entryList.size());
        }

        private void makeIteratorAtIndex(int index)
        {
            iterator = ReverseIterators.reversePeekingIterator(this.entryList.listIterator(index));
        }

        @Override
        public Entry<InternalKey, ByteBuffer> peekPrev()
        {
            return iterator.peekPrev();
        }

        @Override
        public Entry<InternalKey, ByteBuffer> prev()
        {
            return iterator.prev();
        }

        @Override
        public boolean hasPrev()
        {
            return iterator.hasPrev();
        }

        @Override
        public void close()
        {
            release();
        }
    }

    private static class MemTableCleaner
            implements Closeable
    {
        private final MemTable memTable;
        private final MemoryManager memory;

        public MemTableCleaner(MemTable memTable, MemoryManager memory)
        {
            this.memTable = memTable;
            this.memory = memory;
        }

        @Override
        public void close()
                throws IOException
        {
            for (Entry<InternalKey, ByteBuffer> entry : memTable.simpleIterable()) {
                entry.getKey().free(memory);
                memory.free(entry.getValue());
            }
        }
    }
}
