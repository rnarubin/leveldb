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
package com.cleversafe.leveldb.impl;

import com.cleversafe.leveldb.WriteBatch;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

abstract class WriteBatchImpl
        implements WriteBatch
{
    private int approximateSize;
    
    WriteBatchImpl()
    {
        this(0);
    }

    WriteBatchImpl(int initApproxSize)
    {
        this.approximateSize = initApproxSize;
    }

    public final int getApproximateSize()
    {
        return approximateSize;
    }

    @Override
    public void close()
    {
    }


    @Override
    public final WriteBatchImpl put(byte[] key, byte[] value)
    {
        Preconditions.checkNotNull(key, "key is null");
        Preconditions.checkNotNull(value, "value is null");
        return put(ByteBuffer.wrap(key), ByteBuffer.wrap(value));
    }

    @Override
    public final WriteBatchImpl put(ByteBuffer key, ByteBuffer value)
    {
        Preconditions.checkNotNull(key, "key is null");
        Preconditions.checkNotNull(value, "value is null");
        approximateSize += 12 + key.remaining() + value.remaining();
        putInternal(key, value);
        return this;
    }

    @Override
    public final WriteBatchImpl delete(byte[] key)
    {
        Preconditions.checkNotNull(key, "key is null");
        return delete(ByteBuffer.wrap(key));
    }

    @Override
    public final WriteBatchImpl delete(ByteBuffer key)
    {
        Preconditions.checkNotNull(key, "key is null");
        approximateSize += 6 + key.remaining();
        deleteInternal(key);
        return this;
    }

    public abstract int size();

    abstract void putInternal(ByteBuffer key, ByteBuffer value);

    abstract void deleteInternal(ByteBuffer key);

    abstract void forEach(Handler handler);

    interface Handler
    {
        void put(ByteBuffer key, ByteBuffer value);

        void delete(ByteBuffer key);
    }
    
    static final class WriteBatchMulti
            extends WriteBatchImpl
    {
        private final List<Entry<ByteBuffer, ByteBuffer>> batch = new ArrayList<>();;

        @Override
        public int size()
        {
            return batch.size();
        }

        @Override
        void putInternal(ByteBuffer key, ByteBuffer value)
        {
            batch.add(Maps.immutableEntry(key, value));
        }

        @Override
        void deleteInternal(ByteBuffer key)
        {
            batch.add(Maps.immutableEntry(key, (ByteBuffer) null));
        }

        @Override
        public void forEach(Handler handler)
        {
            for (Entry<ByteBuffer, ByteBuffer> entry : batch) {
                ByteBuffer key = entry.getKey();
                ByteBuffer value = entry.getValue();
                if (value != null) {
                    handler.put(key, value);
                }
                else {
                    handler.delete(key);
                }
            }
        }


    }

    static final class WriteBatchSingle
            extends WriteBatchImpl
    {
        private final ByteBuffer key, value;

        WriteBatchSingle(ByteBuffer key, ByteBuffer value)
        {
            super(12 + key.remaining() + value.remaining());
            this.key = key;
            this.value = value;
        }

        WriteBatchSingle(ByteBuffer key)
        {
            super(6 + key.remaining());
            this.key = key;
            this.value = null;
        }

        @Override
        public int size()
        {
            return 1;
        }

        @Override
        public void forEach(Handler handler)
        {
            if (value != null) {
                handler.put(key, value);
            }
            else {
                handler.delete(key);
            }
        }

        @Override
        void putInternal(ByteBuffer key, ByteBuffer value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        void deleteInternal(ByteBuffer key)
        {
            throw new UnsupportedOperationException();
        }
    }
}
