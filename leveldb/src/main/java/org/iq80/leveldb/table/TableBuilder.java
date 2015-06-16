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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import org.iq80.leveldb.Compression;
import org.iq80.leveldb.MemoryManager;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.util.ByteBufferCrc32;
import org.iq80.leveldb.util.ByteBuffers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.iq80.leveldb.impl.VersionSet.TARGET_FILE_SIZE;

public class TableBuilder
{
    /**
     * TABLE_MAGIC_NUMBER was picked by running
     * echo http://code.google.com/p/leveldb/ | sha1sum
     * and taking the leading 64 bits.
     */
    public static final long TABLE_MAGIC_NUMBER = 0xdb4775248b80fb57L;

    private static final Logger LOGGER = LoggerFactory.getLogger(TableBuilder.class);

    private final int blockRestartInterval;
    private final int blockSize;
    private final Compression compression;

    private final FileChannel fileChannel;
    private final BlockBuilder dataBlockBuilder;
    private final BlockBuilder indexBlockBuilder;
    private ByteBuffer lastKey;
    private final UserComparator userComparator;

    private long entryCount;

    // Either Finish() or Abandon() has been called.
    private boolean closed;

    // We do not emit the index entry for a block until we have seen the
    // first key for the next data block.  This allows us to use shorter
    // keys in the index block.  For example, consider a block boundary
    // between the keys "the quick brown fox" and "the who".  We can use
    // "the r" as the key for the index block entry since it is >= all
    // entries in the first block and < all entries in subsequent
    // blocks.
    private boolean pendingIndexEntry;
    private BlockHandle pendingHandle;  // Handle to add to index block

    private long position;

    private final MemoryManager memory;

    public TableBuilder(Options options, FileChannel fileChannel, UserComparator userComparator)
    {
        Preconditions.checkNotNull(options, "options is null");
        Preconditions.checkNotNull(fileChannel, "fileChannel is null");
        try {
            Preconditions.checkState(position == fileChannel.position(), "Expected position %s to equal fileChannel.position %s", position, fileChannel.position());
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        this.memory = options.memoryManager();
        this.fileChannel = fileChannel;
        this.userComparator = userComparator;

        blockRestartInterval = options.blockRestartInterval();
        blockSize = options.blockSize();
        compression = options.compression();

        dataBlockBuilder = new BlockBuilder((int) Math.min(blockSize * 1.1, TARGET_FILE_SIZE), blockRestartInterval,
                userComparator, this.memory);

        // with expected 50% compression
        int expectedNumberOfBlocks = 1024;
        indexBlockBuilder = new BlockBuilder(BlockHandle.MAX_ENCODED_LENGTH * expectedNumberOfBlocks, 1,
                userComparator, this.memory);

        lastKey = ByteBuffers.EMPTY_BUFFER;
    }

    public long getEntryCount()
    {
        return entryCount;
    }

    public long getFileSize()
    {
        return position + dataBlockBuilder.currentSizeEstimate();
    }

    public void add(BlockEntry blockEntry)
            throws IOException
    {
        Preconditions.checkNotNull(blockEntry, "blockEntry is null");
        add(blockEntry.getKey(), blockEntry.getValue());
    }

    public void add(ByteBuffer key, ByteBuffer value)
            throws IOException
    {
        Preconditions.checkNotNull(key, "key is null");
        Preconditions.checkNotNull(value, "value is null");

        Preconditions.checkState(!closed, "table is finished");

        if (entryCount > 0) {
            Preconditions.checkState(userComparator.compare(key, lastKey) > 0, "key must be greater than last key");
        }

        // If we just wrote a block, we can now add the handle to index block
        if (pendingIndexEntry) {
            Preconditions.checkState(dataBlockBuilder.isEmpty(), "Internal error: Table has a pending index entry but data block builder is empty");

            ByteBuffer shortestSeparator = userComparator.findShortestSeparator(lastKey, key);

            ByteBuffer handleEncoding = BlockHandle.writeBlockHandleTo(pendingHandle,
                    memory.allocate(BlockHandle.MAX_ENCODED_LENGTH));
            handleEncoding.flip();
            indexBlockBuilder.add(shortestSeparator, handleEncoding);
            pendingIndexEntry = false;
        }

        lastKey = key;
        entryCount++;
        dataBlockBuilder.add(key, value);

        int estimatedBlockSize = dataBlockBuilder.currentSizeEstimate();
        if (estimatedBlockSize >= blockSize) {
            flush();
        }
    }

    private void flush()
            throws IOException
    {
        Preconditions.checkState(!closed, "table is finished");
        if (dataBlockBuilder.isEmpty()) {
            return;
        }

        Preconditions.checkState(!pendingIndexEntry, "Internal error: Table already has a pending index entry to flush");

        pendingHandle = writeBlock(dataBlockBuilder);
        pendingIndexEntry = true;
    }

    private BlockHandle writeBlock(BlockBuilder blockBuilder)
            throws IOException
    {
        // close the block
        final ByteBuffer raw = blockBuilder.finish();
        ByteBuffer blockContents = raw;
        byte compressionId = 0;
        // attempt to compress the block
        if (compression != null) {
            // include space for packing the block trailer
            ByteBuffer compressedContents = memory.allocate(compression.maxCompressedLength(raw)
                    + BlockTrailer.ENCODED_LENGTH);

            try {
                // can't trust user code, duplicate buffers
                int compressedSize = compression.compress(ByteBuffers.duplicate(raw),
                        ByteBuffers.duplicate(compressedContents));

                // Don't use the compressed data if compressed less than 12.5%,
                if (compressedSize < raw.remaining() - (raw.remaining() / 8)) {
                    compressedContents.limit(compressedContents.position() + compressedSize);
                    blockContents = compressedContents;
                    compressionId = compression.persistentId();
                }
            }
            catch (Exception e) {
                LOGGER.warn("Compression failed", e);
                // compression failed, so just store uncompressed form
                blockContents = raw;
            }
        }

        // create block trailer
        BlockTrailer blockTrailer = new BlockTrailer(compressionId, crc32c(blockContents, compressionId));

        // trailer space was reserved either in compressed output or finished block
        blockContents.mark().position(blockContents.limit()).limit(blockContents.limit() + BlockTrailer.ENCODED_LENGTH);
        BlockTrailer.writeBlockTrailer(blockTrailer, blockContents);
        blockContents.reset();

        // create a handle to this block
        BlockHandle blockHandle = new BlockHandle(position, blockContents.remaining() - BlockTrailer.ENCODED_LENGTH);

        // write data and trailer
        position += fileChannel.write(blockContents);

        // clean up state
        blockBuilder.reset();

        return blockHandle;
    }

    public void finish()
            throws IOException
    {
        Preconditions.checkState(!closed, "table is finished");

        // flush current data block
        flush();

        // mark table as closed
        closed = true;

        // write (empty) meta index block
        BlockBuilder metaIndexBlockBuilder = new BlockBuilder(256, blockRestartInterval, new BytewiseComparator(memory),
                this.memory);
        // TODO(postrelease): Add stats and other meta blocks
        BlockHandle metaindexBlockHandle = writeBlock(metaIndexBlockBuilder);

        // add last handle to index block
        if (pendingIndexEntry) {
            ByteBuffer shortSuccessor = userComparator.findShortSuccessor(lastKey);

            ByteBuffer handleEncoding = BlockHandle.writeBlockHandleTo(pendingHandle,
                    this.memory.allocate(BlockHandle.MAX_ENCODED_LENGTH));
            handleEncoding.flip();
            indexBlockBuilder.add(shortSuccessor, handleEncoding);
            pendingIndexEntry = false;
        }

        // write index block
        BlockHandle indexBlockHandle = writeBlock(indexBlockBuilder);

        // write footer
        Footer footer = new Footer(metaindexBlockHandle, indexBlockHandle);
        ByteBuffer footerEncoding = Footer.writeFooter(footer, this.memory.allocate(Footer.ENCODED_LENGTH));
        footerEncoding.flip();
        position += fileChannel.write(footerEncoding);
    }

    public void abandon()
    {
        Preconditions.checkState(!closed, "table is finished");
        closed = true;
    }

    public static int crc32c(ByteBuffer data, byte compressionId)
    {
        ByteBufferCrc32 crc32 = ByteBuffers.crc32();
        crc32.update(data, data.position(), data.remaining());
        crc32.update(compressionId & 0xFF);
        return ByteBuffers.maskChecksum(crc32.getIntValue());
    }
}
