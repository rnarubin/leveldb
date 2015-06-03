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
import com.google.common.collect.Maps;

import org.iq80.leveldb.WriteBatch;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map.Entry;

import static com.google.common.collect.Lists.newArrayList;

public class WriteBatchImpl
        implements WriteBatch
{
    private final List<Entry<ByteBuffer, ByteBuffer>> batch;
    protected int approximateSize;
    
    public WriteBatchImpl()
    {
        batch = newArrayList();
    }

    public int getApproximateSize()
    {
        return approximateSize;
    }

    public int size()
    {
        return batch.size();
    }

    @Override
    public WriteBatchImpl put(byte[] key, byte[] value)
    {
        return put(ByteBuffer.wrap(key), ByteBuffer.wrap(value));
    }

    public WriteBatchImpl put(ByteBuffer key, ByteBuffer value)
    {
        Preconditions.checkNotNull(key, "key is null");
        Preconditions.checkNotNull(value, "value is null");
        batch.add(Maps.immutableEntry(key, value));
        approximateSize += 12 + key.remaining() + value.remaining();
        return this;
    }

    @Override
    public WriteBatchImpl delete(byte[] key)
    {
        return delete(ByteBuffer.wrap(key));
    }

    public WriteBatchImpl delete(ByteBuffer key)
    {
        Preconditions.checkNotNull(key, "key is null");
        batch.add(Maps.immutableEntry(key, (ByteBuffer) null));
        approximateSize += 6 + key.remaining();
        return this;
    }

    @Override
    public void close()
    {
    }

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

    public interface Handler
    {
        void put(ByteBuffer key, ByteBuffer value);

        void delete(ByteBuffer key);
    }
    
    static class WriteBatchSingle
            extends WriteBatchImpl
    {
        private final ByteBuffer key, value;

        WriteBatchSingle(byte[] key, byte[] value)
        {
            this(ByteBuffer.wrap(key), ByteBuffer.wrap(value));
        }

        WriteBatchSingle(ByteBuffer key, ByteBuffer value)
        {
            this.key = key;
            this.value = value;
            this.approximateSize = 12 + key.remaining() + value.remaining();
        }

        WriteBatchSingle(byte[] key)
        {
            this(ByteBuffer.wrap(key));
        }

        WriteBatchSingle(ByteBuffer key)
        {
            this.key = key;
            this.value = null;
            this.approximateSize = 6 + key.remaining();
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
    }
}
