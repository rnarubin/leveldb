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

import java.nio.ByteBuffer;

/**
 * @deprecated use e.g. {@link Snappy#instance} or {@code null} in conjunction with {@link Options#compression(Compression)}
 */
public enum CompressionType
        implements Compression
{
    NONE, SNAPPY;

    /*
     * this class remains mostly as a stub to bridge the api/impl packages for Options' non-null default compression
     */

    @Override
    public byte persistentId()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compress(ByteBuffer src, ByteBuffer dst)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int uncompress(ByteBuffer src, ByteBuffer dst)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int maxUncompressedLength(ByteBuffer compressed)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int maxCompressedLength(ByteBuffer uncompressed)
    {
        throw new UnsupportedOperationException();
    }
}
