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

import org.iq80.leveldb.util.ByteBuffers;

public class LookupResult
{
    public static LookupResult ok(LookupKey key, ByteBuffer value, boolean needsFreeing)
    {
        return new LookupResult(key, value, false, needsFreeing);
    }

    public static LookupResult deleted(LookupKey key)
    {
        return new LookupResult(key, null, true, false);
    }

    private final LookupKey key;
    private final ByteBuffer value;
    private final boolean deleted;
    private final boolean needsFreeing;

    private LookupResult(LookupKey key, ByteBuffer value, boolean deleted, boolean needsFreeing)
    {
        Preconditions.checkNotNull(key, "key is null");
        this.needsFreeing = needsFreeing;
        this.key = key;
        if (value != null) {
            this.value = needsFreeing ? value : ByteBuffers.duplicate(value);
        }
        else {
            this.value = null;
        }
        this.deleted = deleted;
    }

    public LookupKey getKey()
    {
        return key;
    }

    public ByteBuffer getValue()
    {
        return value;
    }

    public boolean isDeleted()
    {
        return deleted;
    }

    public boolean needsFreeing()
    {
        return needsFreeing;
    }
}
