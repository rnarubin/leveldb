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
package com.cleversafe.leveldb.table;

import java.nio.ByteBuffer;

public class BlockTrailer
{
    public static final int ENCODED_LENGTH = 5;

    private final byte compressionId;
    private final int crc32c;

    public BlockTrailer(byte compressionId, int crc32c)
    {
        this.compressionId = compressionId;
        this.crc32c = crc32c;
    }

    public byte getCompressionId()
    {
        return compressionId;
    }

    public int getCrc32c()
    {
        return crc32c;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("BlockTrailer");
        sb.append("{compressionId=").append(compressionId);
        sb.append(", crc32c=0x").append(Integer.toHexString(crc32c));
        sb.append('}');
        return sb.toString();
    }

    public static BlockTrailer readBlockTrailer(ByteBuffer buffer)
    {
        byte compressionType = buffer.get();
        int crc32c = buffer.getInt();
        return new BlockTrailer(compressionType, crc32c);
    }

    public static ByteBuffer writeBlockTrailer(BlockTrailer blockTrailer, ByteBuffer buffer)
    {
        buffer.put(blockTrailer.getCompressionId());
        buffer.putInt(blockTrailer.getCrc32c());
        return buffer;
    }
}
