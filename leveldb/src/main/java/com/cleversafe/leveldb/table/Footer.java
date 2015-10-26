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

import static com.cleversafe.leveldb.table.BlockHandle.readBlockHandle;
import static com.cleversafe.leveldb.table.BlockHandle.writeBlockHandleTo;
import static com.cleversafe.leveldb.util.SizeOf.SIZE_OF_LONG;

import java.nio.ByteBuffer;

import com.cleversafe.leveldb.util.ByteBuffers;
import com.google.common.base.Preconditions;

public class Footer {
  /**
   * TABLE_MAGIC_NUMBER was picked by running echo http://code.google.com/p/leveldb/ | sha1sum and
   * taking the leading 64 bits.
   */
  private static final long TABLE_MAGIC_NUMBER = 0xdb4775248b80fb57L;

  public static final int ENCODED_LENGTH = (BlockHandle.MAX_ENCODED_LENGTH * 2) + SIZE_OF_LONG;

  private final BlockHandle metaindexBlockHandle;
  private final BlockHandle indexBlockHandle;

  Footer(final BlockHandle metaindexBlockHandle, final BlockHandle indexBlockHandle) {
    this.metaindexBlockHandle = metaindexBlockHandle;
    this.indexBlockHandle = indexBlockHandle;
  }

  public BlockHandle getMetaindexBlockHandle() {
    return metaindexBlockHandle;
  }

  public BlockHandle getIndexBlockHandle() {
    return indexBlockHandle;
  }

  public static Footer readFooter(final ByteBuffer buffer) {
    Preconditions.checkArgument(buffer.remaining() == ENCODED_LENGTH,
        "Expected slice.size to be %s but was %s", ENCODED_LENGTH, buffer.remaining());

    // read metaindex and index handles
    final BlockHandle metaindexBlockHandle = readBlockHandle(buffer);
    final BlockHandle indexBlockHandle = readBlockHandle(buffer);

    // verify magic number
    final long magicNumber = buffer.getLong(buffer.limit() - SIZE_OF_LONG);
    Preconditions.checkArgument(magicNumber == TABLE_MAGIC_NUMBER,
        "File is not a table (bad magic number)");

    return new Footer(metaindexBlockHandle, indexBlockHandle);
  }

  public static ByteBuffer writeFooter(final Footer footer, final ByteBuffer buffer) {
    // remember the starting write index so we can calculate the padding
    final int startingWriteIndex = buffer.position();

    // write metaindex and index handles
    writeBlockHandleTo(footer.getMetaindexBlockHandle(), buffer);
    writeBlockHandleTo(footer.getIndexBlockHandle(), buffer);

    // write padding
    buffer.put(ByteBuffers.duplicateByLength(ByteBuffers.ZERO64, 0,
        ENCODED_LENGTH - SIZE_OF_LONG - (buffer.position() - startingWriteIndex)));

    // write magic number as two (little endian) integers
    buffer.putLong(TABLE_MAGIC_NUMBER);
    return buffer;
  }
}
