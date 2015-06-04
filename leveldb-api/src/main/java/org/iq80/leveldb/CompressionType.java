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
package org.iq80.leveldb;

public enum CompressionType
{
    NONE((byte) 0x00), SNAPPY((byte) 0x01);

    private static final CompressionType[] indexedTypes = new CompressionType[2];
    static {
        for (CompressionType type : CompressionType.values()) {
            indexedTypes[type.persistentId] = type;
        }
    }

    public static CompressionType getCompressionTypeByPersistentId(int persistentId)
    {
        if (persistentId < 0 || persistentId >= indexedTypes.length) {
            throw new IllegalArgumentException("Unknown persistentId " + persistentId);
        }
        return indexedTypes[persistentId];
    }

    private final byte persistentId;

    CompressionType(byte persistentId)
    {
        this.persistentId = persistentId;
    }

    public byte persistentId()
    {
        return persistentId;
    }
}
