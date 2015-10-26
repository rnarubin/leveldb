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

public enum ValueType
{
    DELETION((byte) 0x00), VALUE((byte) 0x01);

    public static ValueType getValueTypeByPersistentId(byte persistentId)
    {
        switch (persistentId) {
            case 0x00:
                return DELETION;
            case 0x01:
                return VALUE;
            default:
                throw new IllegalArgumentException("Unknown persistentId " + persistentId);
        }
    }

    private final byte persistentId;

    ValueType(byte persistentId)
    {
        this.persistentId = persistentId;
    }

    public byte getPersistentId()
    {
        return persistentId;
    }
}
