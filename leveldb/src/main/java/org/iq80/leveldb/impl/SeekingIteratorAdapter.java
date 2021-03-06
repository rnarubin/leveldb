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

import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.Slices;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

public class SeekingIteratorAdapter
        implements DBIterator
{
    private final SnapshotSeekingIterator seekingIterator;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public SeekingIteratorAdapter(SnapshotSeekingIterator seekingIterator)
    {
        this.seekingIterator = seekingIterator;
    }

    @Override
    public void seekToFirst()
    {
        seekingIterator.seekToFirst();
    }

    @Override
    public void seek(byte[] targetKey)
    {
        seekingIterator.seek(Slices.wrappedBuffer(targetKey));
    }

    @Override
    public boolean hasNext()
    {
        return seekingIterator.hasNext();
    }

    @Override
    public DbEntry next()
    {
        return adapt(seekingIterator.next());
    }

    @Override
    public DbEntry peekNext()
    {
        return adapt(seekingIterator.peek());
    }

    @Override
    public void close()
    {
        // This is an end user API.. he might screw up and close multiple times.
        // but we don't want the close multiple times as reference counts go bad.
        if (closed.compareAndSet(false, true)) {
            try {
                seekingIterator.close();
            }
            catch (IOException e) {
                throw new DBException(e);
            }
        }
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    private DbEntry adapt(Entry<Slice, Slice> entry)
    {
        return new DbEntry(entry.getKey(), entry.getValue());
    }

    @Override

    public void seekToLast()
    {
        seekingIterator.seekToLast();
    }

    @Override
    public boolean hasPrev()
    {
        return seekingIterator.hasPrev();
    }

    @Override
    public DbEntry prev()
    {
        return adapt(seekingIterator.prev());
    }

    @Override
    public DbEntry peekPrev()
    {
        return adapt(seekingIterator.peekPrev());
    }

    public static class DbEntry
            implements Entry<byte[], byte[]>
    {
        private final Slice key;
        private final Slice value;

        public DbEntry(Slice key, Slice value)
        {
            Preconditions.checkNotNull(key, "key is null");
            Preconditions.checkNotNull(value, "value is null");
            this.key = key;
            this.value = value;
        }

        @Override
        public byte[] getKey()
        {
            return key.getBytes();
        }

        public Slice getKeySlice()
        {
            return key;
        }

        @Override
        public byte[] getValue()
        {
            return value.getBytes();
        }

        public Slice getValueSlice()
        {
            return value;
        }

        @Override
        public byte[] setValue(byte[] value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object object)
        {
            if (object instanceof Entry) {
                Entry<?, ?> that = (Entry<?, ?>) object;
                return key.equals(that.getKey()) &&
                        value.equals(that.getValue());
            }
            return false;
        }

        @Override
        public int hashCode()
        {
            return key.hashCode() ^ value.hashCode();
        }

        /**
         * Returns a string representation of the form <code>{key}={value}</code>.
         */
        @Override
        public String toString()
        {
            return key + "=" + value;
        }
    }
}
