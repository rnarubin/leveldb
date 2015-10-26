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

import static com.cleversafe.leveldb.impl.LogConstants.BLOCK_SIZE;
import static com.cleversafe.leveldb.impl.LogConstants.HEADER_SIZE;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.cleversafe.leveldb.AsynchronousCloseable;
import com.cleversafe.leveldb.Env.ConcurrentWriteFile;
import com.cleversafe.leveldb.Env.ConcurrentWriteFile.WriteRegion;
import com.cleversafe.leveldb.util.CompletableFutures;
import com.google.common.base.Preconditions;

public final class LogWriter implements AsynchronousCloseable {
  private final ConcurrentWriteFile file;
  private final long fileNumber;

  LogWriter(final ConcurrentWriteFile file, final long fileNumber) {
    Preconditions.checkNotNull(file, "file is null");
    Preconditions.checkArgument(fileNumber >= 0, "fileNumber is negative");

    this.file = file;
    this.fileNumber = fileNumber;
  }

  @Override
  public CompletionStage<Void> asyncClose() {
    return file.asyncClose();
  }

  public long getFileNumber() {
    return fileNumber;
  }

  public CompletionStage<Void> addRecord(final ByteBuffer input, final boolean sync) {
    return file.requestRegion(position -> calculateWriteSize(position, input))
        .thenCompose(region -> CompletableFutures.composeUnconditionally(write(region, input, sync),
            voided -> region.asyncClose()));
  }

  private CompletionStage<Void> write(final WriteRegion region, final ByteBuffer input,
      final boolean sync) {
    // Fragment the record into chunks as necessary and write it. Note that if record
    // is empty, we still want to iterate once to write a single
    // zero-length chunk.

    int blockOffset = (int) (region.startPosition() % BLOCK_SIZE);
    // used to track first, middle and last blocks
    boolean begin = true;
    do {
      int bytesRemainingInBlock = BLOCK_SIZE - blockOffset;
      assert(bytesRemainingInBlock >= 0);

      // Switch to a new block if necessary
      if (bytesRemainingInBlock < HEADER_SIZE) {
        if (bytesRemainingInBlock >= 4) {
          // Fill the rest of the block with zeros
          region.putInt(0);
          bytesRemainingInBlock -= 4;
        }
        switch (bytesRemainingInBlock) {
          // @formatter:off
          case 3: region.put((byte) 0);
          case 2: region.put((byte) 0);
          case 1: region.put((byte) 0);
          default : // 0, do nothing
          // @formatter:on
        }
        blockOffset = 0;
        bytesRemainingInBlock = BLOCK_SIZE - blockOffset;
      }

      // Invariant: we never leave less than HEADER_SIZE bytes available in a block
      final int bytesAvailableInBlock = bytesRemainingInBlock - HEADER_SIZE;
      assert bytesAvailableInBlock >= 0;

      // if there are more bytes in the record then there are available in the block,
      // fragment the record; otherwise write to the end of the record
      boolean end;
      int fragmentLength;
      if (input.remaining() > bytesAvailableInBlock) {
        end = false;
        fragmentLength = bytesAvailableInBlock;
      } else {
        end = true;
        fragmentLength = input.remaining();
      }

      // determine block type
      LogChunkType type;
      if (begin && end) {
        type = LogChunkType.FULL;
      } else if (begin) {
        type = LogChunkType.FIRST;
      } else if (end) {
        type = LogChunkType.LAST;
      } else {
        type = LogChunkType.MIDDLE;
      }

      // write the chunk
      assert blockOffset + HEADER_SIZE <= BLOCK_SIZE;
      final int oldlim = input.limit();
      input.limit(input.position() + fragmentLength);
      blockOffset += appendChunk(region, type, input);
      input.limit(oldlim);

      // we are no longer on the first chunk
      begin = false;
    } while (input.hasRemaining());
    return sync ? region.sync() : CompletableFuture.completedFuture(null);
  }

  private static int appendChunk(final WriteRegion buffer, final LogChunkType type,
      final ByteBuffer data) {
    final int length = data.remaining();
    assert length <= 0xffff : String.format("length %s is larger than two bytes", length);

    final int crc = Logs.getChunkChecksum(type.getPersistentId(), data);

    // Format the header
    buffer.putInt(crc);
    buffer.put((byte) (length & 0xff));
    buffer.put((byte) (length >>> 8));
    buffer.put((byte) (type.getPersistentId()));

    buffer.put(data);

    return HEADER_SIZE + length;
  }

  /**
   * calculates the size of this write's data within the log file given the previous writes
   * position, taking into account new headers and padding around block boundaries
   *
   * @param previousWrite end position of the last record submitted for writing
   * @param newData slice with new data to be written
   * @return this write's size in bytes
   */
  private static int calculateWriteSize(final long previousWrite, final ByteBuffer newData) {
    int firstBlockWrite;
    int dataRemaining = newData.remaining();
    int remainingInBlock = BLOCK_SIZE - (int) (previousWrite % BLOCK_SIZE);
    if (remainingInBlock < HEADER_SIZE) {
      // zero fill
      firstBlockWrite = remainingInBlock;
    } else {
      remainingInBlock -= (firstBlockWrite = HEADER_SIZE);
      if (remainingInBlock >= dataRemaining) {
        // everything fits into the first block
        return firstBlockWrite + dataRemaining;
      }
      firstBlockWrite += remainingInBlock;
      dataRemaining -= remainingInBlock;
    }
    // all subsequent data goes into new blocks
    final int headerCount = dataRemaining / (BLOCK_SIZE - HEADER_SIZE) + 1;
    final int newBlockWrite = dataRemaining + (headerCount * HEADER_SIZE);

    return newBlockWrite + firstBlockWrite;
  }
}
