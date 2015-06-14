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
package org.iq80.leveldb.table;

import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;

import org.iq80.leveldb.util.ByteBuffers;

import static org.iq80.leveldb.table.BlockHandle.readBlockHandle;
import static org.iq80.leveldb.table.BlockHandle.writeBlockHandleTo;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_LONG;

public class Footer
{
    public static final int ENCODED_LENGTH = (BlockHandle.MAX_ENCODED_LENGTH * 2) + SIZE_OF_LONG;

    private final BlockHandle metaindexBlockHandle;
    private final BlockHandle indexBlockHandle;

    Footer(BlockHandle metaindexBlockHandle, BlockHandle indexBlockHandle)
    {
        this.metaindexBlockHandle = metaindexBlockHandle;
        this.indexBlockHandle = indexBlockHandle;
    }

    public BlockHandle getMetaindexBlockHandle()
    {
        return metaindexBlockHandle;
    }

    public BlockHandle getIndexBlockHandle()
    {
        return indexBlockHandle;
    }

    public static Footer readFooter(ByteBuffer buffer)
    {
        Preconditions.checkArgument(buffer.remaining() == ENCODED_LENGTH, "Expected slice.size to be %s but was %s",
                ENCODED_LENGTH, buffer.remaining());

        // read metaindex and index handles
        BlockHandle metaindexBlockHandle = readBlockHandle(buffer);
        BlockHandle indexBlockHandle = readBlockHandle(buffer);

        // verify magic number
        long magicNumber = buffer.getLong(buffer.limit() - SIZE_OF_LONG);
        Preconditions.checkArgument(magicNumber == TableBuilder.TABLE_MAGIC_NUMBER, "File is not a table (bad magic number)");

        return new Footer(metaindexBlockHandle, indexBlockHandle);
    }

    private static final ByteBuffer zeros;
    static {
        ByteBuffer b = ByteBuffer.allocateDirect(ENCODED_LENGTH - SIZE_OF_LONG);
        for (int i = 0; b.hasRemaining(); i++) {
            b.put((byte) 0);
        }
        b.flip();
        zeros = b.asReadOnlyBuffer();
    }

    public static ByteBuffer writeFooter(Footer footer, ByteBuffer buffer)
    {
        // remember the starting write index so we can calculate the padding
        int startingWriteIndex = buffer.position();

        // write metaindex and index handles
        writeBlockHandleTo(footer.getMetaindexBlockHandle(), buffer);
        writeBlockHandleTo(footer.getIndexBlockHandle(), buffer);

        // write padding
        buffer.put(ByteBuffers.duplicateByLength(zeros, 0, ENCODED_LENGTH - SIZE_OF_LONG
                - (buffer.position() - startingWriteIndex)));

        // sliceOutput.writeZero();

        // write magic number as two (little endian) integers
        buffer.putLong(TableBuilder.TABLE_MAGIC_NUMBER);
        // buffer.putInt((int) TableBuilder.TABLE_MAGIC_NUMBER);
        // buffer.putInt((int) (TableBuilder.TABLE_MAGIC_NUMBER >>> 32));
        return buffer;
    }
}
