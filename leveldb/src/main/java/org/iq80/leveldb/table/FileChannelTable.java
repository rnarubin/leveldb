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
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.util.ByteBufferCrc32;
import org.iq80.leveldb.util.ByteBuffers;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Comparator;

public class FileChannelTable
        extends Table
{
    public FileChannelTable(String name, FileChannel fileChannel, Comparator<InternalKey> comparator, Options options)
            throws IOException
    {
        super(name, fileChannel, comparator, options);
    }

    @Override
    protected Footer init()
            throws IOException
    {
        long size = fileChannel.size();
        ByteBuffer footerData = read(size - Footer.ENCODED_LENGTH, Footer.ENCODED_LENGTH);
        return Footer.readFooter(footerData);
    }

    @Override
    protected Block<InternalKey> readBlock(BlockHandle blockHandle)
            throws IOException
    {
        // read block trailer
        ByteBuffer readBuffer = read(blockHandle.getOffset(), blockHandle.getDataSize() + BlockTrailer.ENCODED_LENGTH);
        final int dataStart = readBuffer.position();
        ByteBuffer compressedData = ByteBuffers.duplicateAndAdvance(readBuffer, blockHandle.getDataSize());
        BlockTrailer blockTrailer = BlockTrailer.readBlockTrailer(readBuffer);

        // only verify check sums if explicitly asked by the user
        if (verifyChecksums) {
            // checksum data and the compression type in the trailer
            ByteBufferCrc32 checksum = ByteBuffers.crc32();
            checksum.update(readBuffer, dataStart, blockHandle.getDataSize() + 1);
            int actualCrc32c = ByteBuffers.maskChecksum(checksum.getIntValue());

            Preconditions.checkState(blockTrailer.getCrc32c() == actualCrc32c, "Block corrupted: checksum mismatch");
        }

        return new Block<InternalKey>(uncompressIfNecessary(compressedData, blockTrailer.getCompressionId()),
                comparator, memory, INTERNAL_KEY_DECODER);
    }

    private ByteBuffer read(long offset, int length)
            throws IOException
    {
        ByteBuffer uncompressedBuffer = memory.allocate(length);
        uncompressedBuffer.mark();
        fileChannel.read(uncompressedBuffer, offset);
        if (uncompressedBuffer.hasRemaining()) {
            throw new IOException("Could not read all the data");
        }
        uncompressedBuffer.reset();
        return uncompressedBuffer;
    }
}
