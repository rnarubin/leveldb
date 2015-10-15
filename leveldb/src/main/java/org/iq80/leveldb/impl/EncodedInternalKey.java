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

package org.iq80.leveldb.impl;

import static org.iq80.leveldb.util.SizeOf.SIZE_OF_LONG;

import java.nio.ByteBuffer;

import org.iq80.leveldb.util.ByteBuffers;
import org.iq80.leveldb.util.GrowingBuffer;
import org.iq80.leveldb.util.VariableLengthQuantity;

/**
 * InternalKey that has been read from persisted media (is fully in a data buffer)
 */
public final class EncodedInternalKey extends InternalKey {
  private final ByteBuffer data;

  public EncodedInternalKey(final ByteBuffer data) {
    super(ByteBuffers.duplicate(data, data.position(), data.limit() - SIZE_OF_LONG));
    this.data = data;
  }

  @Override
  public long getSequenceNumber() {
    return SequenceNumber.unpackSequenceNumber(data.getLong(data.limit() - SIZE_OF_LONG));
  }

  @Override
  public ValueType getValueType() {
    return SequenceNumber.unpackValueType(data.getLong(data.limit() - SIZE_OF_LONG));
  }

  @Override
  public ByteBuffer writeToBuffer(final ByteBuffer dst) {
    dst.put(ByteBuffers.duplicate(data));
    return dst;
  }

  @Override
  public ByteBuffer writeUnsharedAndValue(final GrowingBuffer block, final boolean restart,
      final ByteBuffer lastKeyBuffer, final ByteBuffer value) {
    int sharedKeyBytes = 0;
    if (!restart && lastKeyBuffer != null) {
      sharedKeyBytes = ByteBuffers.calculateSharedBytes(data, lastKeyBuffer);
    }

    final int nonSharedKeyBytes = data.remaining() - sharedKeyBytes;

    // write "<shared><non_shared><value_size>"
    VariableLengthQuantity.writeVariableLengthInt(sharedKeyBytes, block);
    VariableLengthQuantity.writeVariableLengthInt(nonSharedKeyBytes, block);
    VariableLengthQuantity.writeVariableLengthInt(value.remaining(), block);

    // write non-shared key bytes
    block.put(
        ByteBuffers.duplicateByLength(data, data.position() + sharedKeyBytes, nonSharedKeyBytes));

    // write value bytes
    block.put(ByteBuffers.duplicate(value));

    return data;
  }
}
