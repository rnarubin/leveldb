/*
 * Copyright (C) 2011 the original author or authors. See the notice.md file distributed with this
 * work for additional information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.cleversafe.leveldb.table;

import static com.cleversafe.leveldb.util.SizeOf.SIZE_OF_INT;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.cleversafe.leveldb.DBComparator;
import com.cleversafe.leveldb.impl.InternalKey;
import com.cleversafe.leveldb.impl.SequenceNumber;
import com.cleversafe.leveldb.impl.TransientInternalKey;
import com.cleversafe.leveldb.impl.ValueType;
import com.cleversafe.leveldb.util.ByteBuffers;
import com.cleversafe.leveldb.util.GrowingBuffer;
import com.cleversafe.leveldb.util.IntVector;
import com.cleversafe.leveldb.util.MemoryManager;
import com.cleversafe.leveldb.util.VariableLengthQuantity;
import com.google.common.base.Preconditions;

public class BlockBuilder implements Closeable {
  private final int blockRestartInterval;
  private final IntVector restartPositions;
  private final DBComparator comparator;
  private final GrowingBuffer block;

  private int restartBlockEntryCount;

  private boolean finished;
  private ByteBuffer lastKeyBuffer;

  public BlockBuilder(final int estimatedSize, final int blockRestartInterval,
      final DBComparator comparator, final MemoryManager memory) {
    Preconditions.checkArgument(estimatedSize >= 0, "estimatedSize is negative");
    Preconditions.checkArgument(blockRestartInterval >= 0, "blockRestartInterval is negative");
    Preconditions.checkNotNull(comparator, "comparator is null");

    this.block = new GrowingBuffer(estimatedSize, memory);
    this.blockRestartInterval = blockRestartInterval;
    this.comparator = comparator;

    restartPositions = new IntVector(32);
    restartPositions.add(0); // first restart point must be 0
  }

  public void reset() {
    block.clear();
    restartPositions.clear();
    restartPositions.add(0); // first restart point must be 0
    restartBlockEntryCount = 0;
    lastKeyBuffer = null;
    finished = false;
  }

  public boolean isEmpty() {
    return lastKeyBuffer == null;
  }

  public int currentSizeEstimate() {
    // no need to estimate if closed
    if (finished) {
      return block.filled();
    }

    // no records is just a single int
    if (block.filled() == 0) {
      return SIZE_OF_INT;
    }

    return block.filled() + // raw data buffer
        restartPositions.size() * SIZE_OF_INT + // restart positions
        SIZE_OF_INT; // restart position size
  }

  public void add(final ByteBuffer key, final ByteBuffer value) {
    // Preconditions.checkState(!finished, "block is finished");
    // Preconditions.checkPositionIndex(restartBlockEntryCount, blockRestartInterval);

    assert isEmpty()
        || comparator.compare(key, lastKeyBuffer) > 0 : "key must be greater than last key";

    int sharedKeyBytes = 0;
    if (restartBlockEntryCount < blockRestartInterval) {
      if (lastKeyBuffer != null) {
        sharedKeyBytes = ByteBuffers.calculateSharedBytes(key, lastKeyBuffer);
      }
    } else {
      // restart prefix compression
      restartPositions.add(block.filled());
      restartBlockEntryCount = 0;
    }

    final int nonSharedKeyBytes = key.remaining() - sharedKeyBytes;

    // write "<shared><non_shared><value_size>"
    VariableLengthQuantity.writeVariableLengthInt(sharedKeyBytes, block);
    VariableLengthQuantity.writeVariableLengthInt(nonSharedKeyBytes, block);
    VariableLengthQuantity.writeVariableLengthInt(value.remaining(), block);

    // write non-shared key bytes
    block.put(ByteBuffers.duplicate(key, sharedKeyBytes, sharedKeyBytes + nonSharedKeyBytes));

    // write value bytes
    block.put(ByteBuffers.duplicate(value));

    // update last key
    lastKeyBuffer = key;

    // update state
    restartBlockEntryCount++;
  }

  public void add(final InternalKey key, final ByteBuffer value) {
    Preconditions.checkNotNull(key, "key is null");
    Preconditions.checkNotNull(value, "value is null");
    Preconditions.checkState(!finished, "block is finished");
    Preconditions.checkPositionIndex(restartBlockEntryCount, blockRestartInterval);

    final boolean restart = restartBlockEntryCount >= blockRestartInterval;
    if (restart) {
      restartPositions.add(block.filled());
      restartBlockEntryCount = 0;
    }
    lastKeyBuffer = key.writeUnsharedAndValue(block, restart, lastKeyBuffer, value);

    // update state
    restartBlockEntryCount++;
  }

  public void addHandle(final InternalKey lastKey, final InternalKey key,
      final BlockHandle handle) {
    final ByteBuffer lastBuffer = ByteBuffers.heapCopy(lastKey.getUserKey());
    final ByteBuffer handleEncoded =
        ByteBuffer.allocate(BlockHandle.MAX_ENCODED_LENGTH).order(ByteOrder.LITTLE_ENDIAN);

    final boolean changed = key == null ? comparator.findShortSuccessor(lastBuffer)
        : comparator.findShortestSeparator(lastBuffer, key.getUserKey());
    final InternalKey encoded = changed
        ? new TransientInternalKey(lastBuffer, SequenceNumber.MAX_SEQUENCE_NUMBER, ValueType.VALUE)
        : lastKey;

    handleEncoded.mark();
    BlockHandle.writeBlockHandleTo(handle, handleEncoded).limit(handleEncoded.position()).reset();
    add(encoded, handleEncoded);
  }

  public ByteBuffer finish() {
    if (!finished) {
      finished = true;

      if (!isEmpty()) {
        restartPositions.write(block);
        block.putInt(restartPositions.size());
      } else {
        block.putInt(0);
      }

      // include space for the trailer rather than writing fragmented buffers
      // this space will be used if compression is not performed
      block.ensureSpace(BlockTrailer.ENCODED_LENGTH);
    }
    return block.get();
  }

  @Override
  public void close() {
    block.close();
  }
}