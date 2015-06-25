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
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.MemoryManager;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.EncodedInternalKey;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.SeekingIterable;
import org.iq80.leveldb.util.ByteBuffers;
import org.iq80.leveldb.util.Closeables;
import org.iq80.leveldb.util.ReferenceCounted;
import org.iq80.leveldb.util.Snappy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Comparator;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Table
        extends ReferenceCounted<Table>
        implements SeekingIterable<InternalKey, ByteBuffer>, AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Table.class);
    protected final String name;
    protected final FileChannel fileChannel;
    protected final Comparator<InternalKey> comparator;
    protected final boolean verifyChecksums;
    protected final Block<InternalKey> indexBlock;
    protected final BlockHandle metaindexBlockHandle;
    protected final MemoryManager memory;
    private final Compression compression;
    protected static final Decoder<InternalKey> INTERNAL_KEY_DECODER = new Decoder<InternalKey>()
    {
        @Override
        public InternalKey decode(ByteBuffer b)
        {
            return new EncodedInternalKey(b, true, false);
        }
    };

    public Table(String name, FileChannel fileChannel, Comparator<InternalKey> comparator, Options options)
            throws IOException
    {
        Preconditions.checkNotNull(name, "name is null");
        Preconditions.checkNotNull(fileChannel, "fileChannel is null");
        long size = fileChannel.size();
        Preconditions.checkArgument(size >= Footer.ENCODED_LENGTH, "File is corrupt: size must be at least %s bytes", Footer.ENCODED_LENGTH);
        Preconditions.checkNotNull(comparator, "comparator is null");

        this.memory = options.memoryManager();
        this.compression = options.compression();

        this.name = name;
        this.fileChannel = fileChannel;
        this.verifyChecksums = options.verifyChecksums();
        this.comparator = comparator;

        Footer footer = init();
        indexBlock = readBlock(footer.getIndexBlockHandle());
        metaindexBlockHandle = footer.getMetaindexBlockHandle();
    }

    protected abstract Footer init()
            throws IOException;

    @Override
    public TableIterator iterator()
    {
        return new TableIterator(this, indexBlock.iterator());
    }

    public Block<InternalKey> openBlock(ByteBuffer blockEntry)
    {
        BlockHandle blockHandle = BlockHandle.readBlockHandle(ByteBuffers.duplicate(blockEntry));
        Block<InternalKey> dataBlock;
        try {
            dataBlock = readBlock(blockHandle);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return dataBlock;
    }

    protected ByteBuffer uncompressIfNecessary(ByteBuffer compressedData, byte compressionId)
    {
        if (compressionId == 0) {
            // not compressed
            return compressedData;
        }
        if (compression != null && compressionId == compression.persistentId()) {
            // id matches user compression
            return uncompress(compression, compressedData, memory);
        }
        if (compressionId == 1) {
            // id matches Snappy, but not user compression, implying legacy data
            return uncompress(Snappy.instance(), compressedData, memory);
        }
        else {
            throw new DBException("Unknown compression identifier: " + compressionId);
        }
    }

    private static ByteBuffer uncompress(Compression compression, ByteBuffer compressedData, MemoryManager memory)
    {
        ByteBuffer dst = memory.allocate(compression.maxUncompressedLength(compressedData));
        final int oldpos = dst.position();
        final int length = compression.uncompress(compressedData, dst);
        dst.limit(oldpos + length).position(oldpos);
        return dst;
    }

    protected abstract Block<InternalKey> readBlock(BlockHandle blockHandle)
            throws IOException;

    /**
     * Given a key, return an approximate byte offset in the file where
     * the data for that key begins (or would begin if the key were
     * present in the file).  The returned value is in terms of file
     * bytes, and so includes effects like compression of the underlying data.
     * For example, the approximate offset of the last key in the table will
     * be close to the file length.
     */
    public long getApproximateOffsetOf(InternalKey key)
    {
        BlockIterator<InternalKey> iterator = indexBlock.iterator();
        iterator.seek(key);
        if (iterator.hasNext()) {
            BlockHandle blockHandle = BlockHandle.readBlockHandle(iterator.next().getValue());
            return blockHandle.getOffset();
        }

        // key is past the last key in the file.  Approximate the offset
        // by returning the offset of the metaindex block (which is
        // right near the end of the file).
        return metaindexBlockHandle.getOffset();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("Table");
        sb.append("{name='").append(name).append('\'');
        sb.append(", comparator=").append(comparator);
        sb.append(", verifyChecksums=").append(verifyChecksums);
        sb.append('}');
        return sb.toString();
    }

    @Override
    protected final Table getThis()
    {
        return this;
    }

    @Override
    protected void dispose()
    {
        try {
            closer().call();
        }
        catch (Exception e) {
            Throwables.propagate(e);
        }
    }

    public Callable<?> closer()
    {
        return new Closer(getReferenceCount(), fileChannel, indexBlock);
    }

    private static class Closer
            implements Callable<Void>
    {
        private final AutoCloseable[] closeables;
        private final AtomicInteger refCount;

        public Closer(AtomicInteger refCount, AutoCloseable... closeables)
        {
            this.refCount = refCount;
            this.closeables = closeables;
        }

        @Override
        public Void call()
        {
            if (refCount.get() != 0) {
                LOGGER.warn("table finalized with {} open refs", refCount.get());
            }
            for (AutoCloseable c : closeables) {
                Closeables.closeQuietly(c);
            }
            return null;
        }
    }

}
