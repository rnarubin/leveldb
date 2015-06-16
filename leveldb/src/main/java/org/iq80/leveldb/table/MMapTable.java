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

import org.iq80.leveldb.Options;
import org.iq80.leveldb.util.ByteBufferCrc32;
import org.iq80.leveldb.util.ByteBuffers;

import com.google.common.base.Preconditions;

import org.iq80.leveldb.util.Closeables;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Comparator;
import java.util.concurrent.Callable;


public class MMapTable
        extends Table
{
    private MappedByteBuffer data;

    public MMapTable(String name, FileChannel fileChannel, Comparator<ByteBuffer> comparator, Options options)
            throws IOException
    {
        super(name, fileChannel, comparator, options);
        long size = fileChannel.size();
        Preconditions.checkArgument(size <= Integer.MAX_VALUE, "File must be smaller than %s bytes", Integer.MAX_VALUE);
    }

    @Override
    protected Footer init()
            throws IOException
    {
        long fsize = fileChannel.size();
        assert fsize <= Integer.MAX_VALUE;
        int size = (int) fsize;
        data = fileChannel.map(MapMode.READ_ONLY, 0, size);
        data.order(ByteOrder.LITTLE_ENDIAN);
        ByteBuffer footerSlice = ByteBuffers.duplicate(data, size - Footer.ENCODED_LENGTH, size);
        return Footer.readFooter(footerSlice);
    }

    @Override
    public Callable<?> closer()
    {
        return new Closer(name, fileChannel, data);
    }

    private static class Closer
            implements Callable<Void>
    {
        // private final String name;
        private final Closeable closeable;
        private final MappedByteBuffer data;

        public Closer(String name, Closeable closeable, MappedByteBuffer data)
        {
            this.closeable = closeable;
            this.data = data;
        }

        public Void call()
                throws IOException
        {
            ByteBuffers.unmap(data);
            Closeables.closeQuietly(closeable);
            return null;
        }
    }

    @Override
    protected Block readBlock(BlockHandle blockHandle)
            throws IOException
    {
        // read block trailer
        BlockTrailer blockTrailer = BlockTrailer.readBlockTrailer(ByteBuffers.duplicateByLength(this.data,
                (int) blockHandle.getOffset() + blockHandle.getDataSize(),
                BlockTrailer.ENCODED_LENGTH));

        // only verify check sums if explicitly asked by the user
        if (verifyChecksums) {
            // checksum data and the compression type in the trailer
            ByteBufferCrc32 checksum = ByteBuffers.crc32();
            checksum.update(data, (int) blockHandle.getOffset(), blockHandle.getDataSize() + 1);
            int actualCrc32c = ByteBuffers.maskChecksum(checksum.getIntValue());

            Preconditions.checkState(blockTrailer.getCrc32c() == actualCrc32c, "Block corrupted: checksum mismatch");
        }

        return new Block(uncompressIfNecessary(
                read(this.data, (int) blockHandle.getOffset(), blockHandle.getDataSize()),
                blockTrailer.getCompressionId()), comparator, memory);
    }

    public static ByteBuffer read(MappedByteBuffer data, int offset, int length)
    {
        return ByteBuffers.duplicateByLength(data, data.position() + offset, length);
    }
}
