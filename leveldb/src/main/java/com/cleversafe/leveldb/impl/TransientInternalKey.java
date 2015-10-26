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

package com.cleversafe.leveldb.impl;

import static com.cleversafe.leveldb.util.SizeOf.SIZE_OF_LONG;

import java.nio.ByteBuffer;

import com.cleversafe.leveldb.util.ByteBuffers;
import com.cleversafe.leveldb.util.GrowingBuffer;
import com.cleversafe.leveldb.util.VariableLengthQuantity;

/**
 * InternalKey from a top-edge layer (has not been persisted yet, sequence and ValueType not yet
 * encoded)
 */
public final class TransientInternalKey extends InternalKey {
  private final long sequenceNumber;
  private final ValueType valueType;

  public TransientInternalKey(final ByteBuffer userKey, final long sequenceNumber,
      final ValueType valueType) {
    super(userKey);
    this.sequenceNumber = sequenceNumber;
    this.valueType = valueType;

  }

  @Override
  public long getSequenceNumber() {
    return sequenceNumber;
  }

  @Override
  public ValueType getValueType() {
    return valueType;
  }

  @Override
  public ByteBuffer writeToBuffer(final ByteBuffer dst) {
    dst.put(ByteBuffers.duplicate(userKey));
    dst.putLong(SequenceNumber.packSequenceAndValueType(sequenceNumber, valueType));
    return dst;
  }

  @Override
  public ByteBuffer writeUnsharedAndValue(final GrowingBuffer block, final boolean restart,
      final ByteBuffer lastKeyBuffer, final ByteBuffer value) {
    int sharedKeyBytes = 0;
    if (!restart && lastKeyBuffer != null) {
      sharedKeyBytes = ByteBuffers.calculateSharedBytes(userKey, lastKeyBuffer);
    }

    final int nonSharedKeyBytes = userKey.remaining() + SIZE_OF_LONG - sharedKeyBytes;

    // write "<shared><non_shared><value_size>"
    VariableLengthQuantity.writeVariableLengthInt(sharedKeyBytes, block);
    VariableLengthQuantity.writeVariableLengthInt(nonSharedKeyBytes, block);
    VariableLengthQuantity.writeVariableLengthInt(value.remaining(), block);

    // write non-shared key bytes
    block.put(ByteBuffers.duplicateByLength(userKey, userKey.position() + sharedKeyBytes,
        nonSharedKeyBytes - SIZE_OF_LONG));
    block.putLong(SequenceNumber.packSequenceAndValueType(sequenceNumber, valueType));

    // write value bytes
    block.put(ByteBuffers.duplicate(value));

    return userKey;
  }
}
