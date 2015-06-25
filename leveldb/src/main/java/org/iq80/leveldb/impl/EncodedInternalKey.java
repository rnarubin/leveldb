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

import org.iq80.leveldb.util.ByteBuffers;
import org.iq80.leveldb.util.GrowingBuffer;
import org.iq80.leveldb.util.VariableLengthQuantity;

import com.google.common.base.Preconditions;

import static org.iq80.leveldb.util.SizeOf.SIZE_OF_LONG;

/**
 * InternalKey that has been read from persisted media (is fully in a data buffer)
 */
public class EncodedInternalKey
        extends InternalKey
{
    private final ByteBuffer userKey, data;

    public EncodedInternalKey(ByteBuffer data)
    {
        this(data, false);
    }

    public EncodedInternalKey(ByteBuffer data, boolean freeKey)
    {
        this(data, freeKey, false);
    }

    public EncodedInternalKey(ByteBuffer data, boolean freeKey, boolean freeValue)
    {
        super(freeKey, freeValue);
        Preconditions.checkNotNull(data, "data is null");
        Preconditions.checkArgument(data.remaining() >= SIZE_OF_LONG, "data must be at least %s bytes", SIZE_OF_LONG);
        this.userKey = ByteBuffers.duplicate(data, data.position(), data.limit() - SIZE_OF_LONG);
        this.data = data;
    }

    @Override
    public ByteBuffer getUserKey()
    {
        return userKey;
    }

    @Override
    public long getSequenceNumber()
    {
        return SequenceNumber.unpackSequenceNumber(data.getLong(data.limit() - SIZE_OF_LONG));
    }

    @Override
    public ValueType getValueType()
    {
        return SequenceNumber.unpackValueType(data.getLong(data.limit() - SIZE_OF_LONG));
    }

    @Override
    public ByteBuffer writeToBuffer(ByteBuffer dst)
    {
        dst.put(ByteBuffers.duplicate(data));
        return dst;
    }

    @Override
    public ByteBuffer writeUnsharedAndValue(GrowingBuffer block,
            boolean restart,
            ByteBuffer lastKeyBuffer,
            ByteBuffer value)
    {
        int sharedKeyBytes = 0;
        if (!restart && lastKeyBuffer != null) {
            sharedKeyBytes = ByteBuffers.calculateSharedBytes(data, lastKeyBuffer);
        }

        int nonSharedKeyBytes = data.remaining() - sharedKeyBytes;

        // write "<shared><non_shared><value_size>"
        VariableLengthQuantity.writeVariableLengthInt(sharedKeyBytes, block);
        VariableLengthQuantity.writeVariableLengthInt(nonSharedKeyBytes, block);
        VariableLengthQuantity.writeVariableLengthInt(value.remaining(), block);

        // write non-shared key bytes
        block.put(ByteBuffers.duplicateByLength(data, data.position() + sharedKeyBytes, nonSharedKeyBytes));

        // write value bytes
        block.put(ByteBuffers.duplicate(value));

        return data;
    }
}


