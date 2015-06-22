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

import org.iq80.leveldb.DBBufferComparator;
import org.iq80.leveldb.MemoryManager;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.SequenceNumber;
import org.iq80.leveldb.impl.TransientInternalKey;
import org.iq80.leveldb.impl.ValueType;
import org.iq80.leveldb.util.ByteBuffers;
import org.iq80.leveldb.util.ByteBuffers.CloseableByteBuffer;
import org.iq80.leveldb.util.GrowingBuffer;
import org.iq80.leveldb.util.IntVector;

import java.nio.ByteBuffer;

import static org.iq80.leveldb.util.SizeOf.SIZE_OF_INT;

public class BlockBuilder
{
    private final int blockRestartInterval;
    private final IntVector restartPositions;
    private final DBBufferComparator comparator;

    private int entryCount;
    private int restartBlockEntryCount;

    private boolean finished;
    private final GrowingBuffer block;
    private ByteBuffer lastKeyBuffer;
    private final MemoryManager memory;

    public BlockBuilder(int estimatedSize,
            int blockRestartInterval,
 DBBufferComparator comparator,
            MemoryManager memory)
    {
        Preconditions.checkArgument(estimatedSize >= 0, "estimatedSize is negative");
        Preconditions.checkArgument(blockRestartInterval >= 0, "blockRestartInterval is negative");
        Preconditions.checkNotNull(comparator, "comparator is null");

        this.memory = memory;
        this.block = new GrowingBuffer(estimatedSize, this.memory);
        this.blockRestartInterval = blockRestartInterval;
        this.comparator = comparator;

        restartPositions = new IntVector(32);
        restartPositions.add(0);  // first restart point must be 0
    }

    public void reset()
    {
        block.clear();
        entryCount = 0;
        restartPositions.clear();
        restartPositions.add(0); // first restart point must be 0
        restartBlockEntryCount = 0;
        lastKeyBuffer = null;
        finished = false;
    }

    public int getEntryCount()
    {
        return entryCount;
    }

    public boolean isEmpty()
    {
        return entryCount == 0;
    }

    public int currentSizeEstimate()
    {
        // no need to estimate if closed
        if (finished) {
            return block.filled();
        }

        // no records is just a single int
        if (block.filled() == 0) {
            return SIZE_OF_INT;
        }

        return block.filled() +                              // raw data buffer
                restartPositions.size() * SIZE_OF_INT +    // restart positions
                SIZE_OF_INT;                               // restart position size
    }

    public void add(InternalKey key, ByteBuffer value)
    {
        Preconditions.checkNotNull(key, "key is null");
        Preconditions.checkNotNull(value, "value is null");
        Preconditions.checkState(!finished, "block is finished");
        Preconditions.checkPositionIndex(restartBlockEntryCount, blockRestartInterval);

        Preconditions.checkArgument(lastKeyBuffer == null || comparator.compare(key.getUserKey(), lastKeyBuffer) > 0,
                "key must be greater than last key");

        // int sharedKeyBytes = 0;
        // if (restartBlockEntryCount < blockRestartInterval) {
        // if (lastKeyBuffer != null) {
        // sharedKeyBytes = ByteBuffers.calculateSharedBytes(key, lastKeyBuffer);
        // }
        // }
        // else {
        // // restart prefix compression
        // restartPositions.add(block.filled());
        // restartBlockEntryCount = 0;
        // }
        //
        // int nonSharedKeyBytes = key.remaining() - sharedKeyBytes;
        //
        // // write "<shared><non_shared><value_size>"
        // VariableLengthQuantity.writeVariableLengthInt(sharedKeyBytes, block);
        // VariableLengthQuantity.writeVariableLengthInt(nonSharedKeyBytes, block);
        // VariableLengthQuantity.writeVariableLengthInt(value.remaining(), block);
        //
        // // write non-shared key bytes
        // block.put(ByteBuffers.duplicate(key, sharedKeyBytes, sharedKeyBytes + nonSharedKeyBytes));
        //
        // // write value bytes
        // block.put(ByteBuffers.duplicate(value));
        //
        // // update last key
        // lastKeyBuffer = key;
        boolean restart = restartBlockEntryCount >= blockRestartInterval;
        if (restart) {
            restartPositions.add(block.filled());
            restartBlockEntryCount = 0;
        }
        lastKeyBuffer = key.writeUnsharedAndValue(block, restart, lastKeyBuffer, value);

        // update state
        entryCount++;
        restartBlockEntryCount++;
    }

    public void addHandle(InternalKey lastKey, InternalKey key, BlockHandle handle)
    {
        try (CloseableByteBuffer last = ByteBuffers.closeable(ByteBuffers.copy(lastKey.getUserKey(), memory), memory);
                CloseableByteBuffer handleEncoded = ByteBuffers.closeable(
                        memory.allocate(BlockHandle.MAX_ENCODED_LENGTH), memory)) {
            boolean changed = key == null ? comparator.findShortSuccessor(last.buffer)
                    : comparator.findShortestSeparator(last.buffer, key.getUserKey());

            InternalKey encoded = changed ? new TransientInternalKey(last.buffer, SequenceNumber.MAX_SEQUENCE_NUMBER,
                    ValueType.VALUE) : key;

            handleEncoded.buffer.mark();
            BlockHandle.writeBlockHandleTo(handle, handleEncoded.buffer).limit(handleEncoded.buffer.position()).reset();
            add(encoded, handleEncoded.buffer);
        }
        // ByteBuffer shortestSeparator = userComparator.findShortestSeparator(lastKey, key);
        //
        // ByteBuffer handleEncoding = memory.allocate(BlockHandle.MAX_ENCODED_LENGTH);
        // handleEncoding.mark();
        // BlockHandle.writeBlockHandleTo(pendingHandle, handleEncoding);
        // handleEncoding.limit(handleEncoding.position()).reset();
        // indexBlockBuilder.add(shortestSeparator, handleEncoding);
    }

    public ByteBuffer finish()
    {
        if (!finished) {
            finished = true;

            if (entryCount > 0) {
                restartPositions.write(block);
                block.putInt(restartPositions.size());
            }
            else {
                block.putInt(0);
            }

            // include space for the trailer rather than writing fragmented buffers
            // this space will be used if compression is not performed
            block.ensureSpace(BlockTrailer.ENCODED_LENGTH);
        }
        return block.get();
    }
}
