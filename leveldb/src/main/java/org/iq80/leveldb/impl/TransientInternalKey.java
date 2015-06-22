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
 * InternalKey from a top-edge layer (has not been persisted yet, sequence and ValueType not yet encoded)
 */
public class TransientInternalKey
        extends InternalKey
{
    private ByteBuffer userKey;
    private final long sequenceNumber;
    private final ValueType valueType;

    public TransientInternalKey(ByteBuffer userKey, long sequenceNumber, ValueType valueType)
    {
        Preconditions.checkNotNull(userKey, "userKey is null");
        Preconditions.checkArgument(sequenceNumber >= 0, "sequenceNumber is negative");
        Preconditions.checkNotNull(valueType, "valueType is null");

        this.userKey = userKey;
        this.sequenceNumber = sequenceNumber;
        this.valueType = valueType;
    }

    @Override
    public ByteBuffer getUserKey()
    {
        return userKey;
    }

    @Override
    public long getSequenceNumber()
    {
        return sequenceNumber;
    }

    @Override
    public ValueType getValueType()
    {
        return valueType;
    }

    @Override
    public void writeToBuffer(ByteBuffer dst)
    {
        dst.put(ByteBuffers.duplicate(userKey));
        dst.putLong(SequenceNumber.packSequenceAndValueType(sequenceNumber, valueType));
    }

    @Override
    public ByteBuffer writeUnsharedAndValue(GrowingBuffer block,
            boolean restart,
            ByteBuffer lastKeyBuffer,
            ByteBuffer value)
    {
        int sharedKeyBytes = 0;
        if (!restart && lastKeyBuffer != null) {
            sharedKeyBytes = ByteBuffers.calculateSharedBytes(userKey, lastKeyBuffer);
        }

        int nonSharedKeyBytes = userKey.remaining() + SIZE_OF_LONG - sharedKeyBytes;

        // write "<shared><non_shared><value_size>"
        VariableLengthQuantity.writeVariableLengthInt(sharedKeyBytes, block);
        VariableLengthQuantity.writeVariableLengthInt(nonSharedKeyBytes, block);
        VariableLengthQuantity.writeVariableLengthInt(value.remaining(), block);

        // write non-shared key bytes
        block.put(ByteBuffers.duplicateByLength(userKey, userKey.position() + sharedKeyBytes, nonSharedKeyBytes
                - SIZE_OF_LONG));
        block.putLong(SequenceNumber.packSequenceAndValueType(sequenceNumber, valueType));

        // write value bytes
        block.put(ByteBuffers.duplicate(value));

        return userKey;
    }

}


