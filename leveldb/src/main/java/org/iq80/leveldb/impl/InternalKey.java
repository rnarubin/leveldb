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
import org.iq80.leveldb.util.SizeOf;

public abstract class InternalKey
{
    public static final InternalKey MINIMUM_KEY = new TransientInternalKey(ByteBuffers.EMPTY_BUFFER, 0,
            ValueType.getValueTypeByPersistentId((byte) 0));

    public abstract ByteBuffer getUserKey();

    public abstract long getSequenceNumber();

    public abstract ValueType getValueType();

    public abstract void writeToBuffer(ByteBuffer dst);

    public int getEncodedSize()
    {
        return getUserKey().remaining() + SizeOf.SIZE_OF_LONG;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("InternalKey");
        sb.append("{key=").append(ByteBuffers.toString(getUserKey()));
        sb.append(", sequenceNumber=").append(getSequenceNumber());
        sb.append(", valueType=").append(getValueType());
        sb.append('}');
        return sb.toString();
    }
}
