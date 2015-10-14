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

import static org.iq80.leveldb.impl.LogChunkType.BAD_CHUNK;
import static org.iq80.leveldb.impl.LogChunkType.EOF;
import static org.iq80.leveldb.impl.LogChunkType.UNKNOWN;
import static org.iq80.leveldb.impl.LogChunkType.ZERO_TYPE;
import static org.iq80.leveldb.impl.LogConstants.BLOCK_SIZE;
import static org.iq80.leveldb.impl.LogConstants.HEADER_SIZE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.iq80.leveldb.Env;
import org.iq80.leveldb.Env.AsynchronousCloseableIterator;
import org.iq80.leveldb.Env.SequentialReadFile;
import org.iq80.leveldb.FileInfo;
import org.iq80.leveldb.util.ByteBuffers;
import org.iq80.leveldb.util.CompletableFutures;
import org.iq80.leveldb.util.GrowingBuffer;
import org.iq80.leveldb.util.MemoryManagers;

public class LogReader implements AsynchronousCloseableIterator<ByteBuffer> {

  private final SequentialReadFile file;
  private final LogMonitor monitor;
  private final boolean verifyChecksums;

  /**
   * Scratch buffer in which the next record is assembled.
   */
  private final GrowingBuffer recordScratch;

  /**
   * Scratch buffer for current block. The currentBlock is sliced off the underlying buffer.
   */
  private final ByteBuffer blockScratch;

  /**
   * The current block records are being read from.
   */
  private ByteBuffer currentBlock = ByteBuffers.EMPTY_BUFFER;

  /**
   * Current chunk which is sliced from the current block.
   */
  private ByteBuffer currentChunk = ByteBuffers.EMPTY_BUFFER;

  private LogReader(final SequentialReadFile file, final LogMonitor monitor,
      final boolean verifyChecksums) {
    this.file = file;
    this.monitor = monitor;
    this.verifyChecksums = verifyChecksums;
    this.blockScratch = ByteBuffer.allocateDirect(BLOCK_SIZE).order(ByteOrder.LITTLE_ENDIAN);
    this.recordScratch = new GrowingBuffer(BLOCK_SIZE, MemoryManagers.heap());
  }

  private LogReader() {
    this.file = null;
    this.monitor = null;
    this.verifyChecksums = false;
    this.blockScratch = null;
    this.recordScratch = null;
  }

  private static final LogReader EMPTY_READER = new LogReader() {
    @Override
    public CompletionStage<Optional<ByteBuffer>> next() {
      return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletionStage<Void> asyncClose() {
      return CompletableFuture.completedFuture(null);
    }
  };

  public static CompletionStage<LogReader> newLogReader(final Env env, final FileInfo info,
      final LogMonitor monitor, final boolean verifyChecksums, final long initialOffset) {
    final CompletionStage<LogReader> openReader = env.openSequentialReadFile(info)
        .thenApply(file -> new LogReader(file, monitor, verifyChecksums));

    return openReader.thenCompose(logReader -> {
      if (initialOffset > 0) {
        // if skip encounters an error, but the monitor doesn't throw an exception, close the reader
        // and return an empty one; if the monitor does throw an exception, also close the reader
        // and propagate the exception
        CompletableFutures.composeUnconditionally(logReader.skipToInitialBlock(initialOffset),
            skipSuccess -> skipSuccess.orElse(false) ? openReader
                : logReader.asyncClose().thenApply(voided -> EMPTY_READER));
        return null;
      } else if (initialOffset == 0) {
        // no need to skip, just return
        return openReader;
      } else {
        throw new IllegalArgumentException("file offset < 0:" + initialOffset);
      }
    });
  }

  /**
   * Skips all blocks that are completely before "initial_offset_".
   * <p/>
   * Handles reporting corruption
   *
   * @return true on success.
   */
  private CompletionStage<Boolean> skipToInitialBlock(final long initialOffset) {
    final int offsetInBlock = (int) (initialOffset % BLOCK_SIZE);
    long blockStart = initialOffset - offsetInBlock;

    // Don't search a block if we'd be in the trailer
    if (offsetInBlock > BLOCK_SIZE - 6) {
      blockStart += BLOCK_SIZE;
    }

    final long blockStartLocation = blockStart;
    // Skip to start of first block that can contain the initial record
    return blockStartLocation > 0 ? CompletableFutures
        .handleExceptional(file.skip(blockStartLocation), (voidSuccess, exception) -> {
          if (exception == null) {
            return true;
          } else {
            reportDrop(blockStartLocation, exception);
            return false;
          }
        }) : CompletableFuture.completedFuture(true);
  }

  @Override
  public CompletionStage<Optional<ByteBuffer>> next() {
    recordScratch.clear();
    return nextRecord(false).thenApply(Optional::ofNullable);
  }

  private CompletionStage<ByteBuffer> nextRecord(final boolean inFragmentedRecord) {
    return CompletableFutures.thenComposeExceptional(readNextChunk(), chunkType -> {
      switch (chunkType) {
        case FULL:
          if (inFragmentedRecord) {
            reportCorruption(recordScratch.filled(), "Partial record without end");
            // simply return this full block
          }
          recordScratch.clear();
          return CompletableFuture.completedFuture(ByteBuffers.copyOwned(currentChunk));

        case FIRST:
          if (inFragmentedRecord) {
            reportCorruption(recordScratch.filled(), "Partial record without end");
            // clear the scratch and start over from this chunk
            recordScratch.clear();
          }
          recordScratch.put(currentChunk);
          return nextRecord(true);

        case MIDDLE:
          if (!inFragmentedRecord) {
            reportCorruption(recordScratch.filled(), "Missing start of fragmented record");

            // clear the scratch and skip this chunk
            recordScratch.clear();
          } else {
            recordScratch.put(currentChunk);
          }
          return nextRecord(inFragmentedRecord);

        case LAST:
          if (!inFragmentedRecord) {
            reportCorruption(recordScratch.filled(), "Missing start of fragmented record");

            // clear the scratch and skip this chunk
            recordScratch.clear();
            return nextRecord(inFragmentedRecord);
          } else {
            recordScratch.put(currentChunk);
            return CompletableFuture.completedFuture(ByteBuffers.copyOwned(recordScratch.get()));
          }

        case EOF:
          if (inFragmentedRecord) {
            reportCorruption(recordScratch.filled(), "Partial record without end");

            // clear the scratch and return
            recordScratch.clear();
          }
          return CompletableFuture.completedFuture(null);

        case BAD_CHUNK:
          if (inFragmentedRecord) {
            reportCorruption(recordScratch.filled(), "Error in middle of record");
            recordScratch.clear();
          }
          return nextRecord(false);

        default:
          int dropSize = currentChunk.remaining();
          if (inFragmentedRecord) {
            dropSize += recordScratch.filled();
          }
          reportCorruption(dropSize, String.format("Unexpected chunk type %s", chunkType));
          recordScratch.clear();
          return nextRecord(false);
      }
    });
  }

  /**
   * @return type, or one of the preceding special values
   */
  private CompletionStage<LogChunkType> readNextChunk() {
    // read the next block if necessary
    final CompletionStage<Boolean> readNext = currentBlock.remaining() < HEADER_SIZE
        ? readNextBlock() : CompletableFuture.completedFuture(true);
    currentChunk = ByteBuffers.EMPTY_BUFFER;

    return CompletableFutures.thenApplyExceptional(readNext, hasData -> {
      if (!hasData) {
        return EOF;
      }

      // parse header
      final int expectedChecksum = currentBlock.getInt();
      int length = ByteBuffers.readUnsignedByte(currentBlock);
      length = length | ByteBuffers.readUnsignedByte(currentBlock) << 8;
      final byte chunkTypeId = currentBlock.get();
      final LogChunkType chunkType = LogChunkType.getLogChunkTypeByPersistentId(chunkTypeId);

      // verify length
      if (length > currentBlock.remaining()) {
        final int dropSize = currentBlock.remaining() + HEADER_SIZE;
        reportCorruption(dropSize, "Invalid chunk length");
        currentBlock = ByteBuffers.EMPTY_BUFFER;
        return BAD_CHUNK;
      }

      // skip zero length records
      if (chunkType == ZERO_TYPE && length == 0) {
        // Skip zero length record without reporting any drops since
        // such records are produced by the writing code.
        currentBlock = ByteBuffers.EMPTY_BUFFER;
        return BAD_CHUNK;
      }

      // read the chunk
      currentChunk = ByteBuffers.duplicateAndAdvance(currentBlock, length);

      if (verifyChecksums) {
        final int actualChecksum = Logs.getChunkChecksum(chunkTypeId, currentChunk);
        if (actualChecksum != expectedChecksum) {
          // Drop the rest of the buffer since "length" itself may have
          // been corrupted and if we trust it, we could find some
          // fragment of a real log record that just happens to look
          // like a valid log record.
          final int dropSize = currentBlock.remaining() + HEADER_SIZE;
          currentBlock = ByteBuffers.EMPTY_BUFFER;
          reportCorruption(dropSize, "Invalid chunk checksum");
          return BAD_CHUNK;
        }
      }

      // Skip unknown chunk types
      // Since this comes last we then know it is a valid chunk, and is just a type we don't
      // understand
      if (chunkType == UNKNOWN) {
        reportCorruption(length,
            String.format("Unknown chunk type %d", chunkType.getPersistentId()));
        return BAD_CHUNK;
      }

      return chunkType;
    });
  }

  /**
   * @return hasData
   */
  private CompletionStage<Boolean> readNextBlock() {
    blockScratch.clear();
    return CompletableFutures.handleExceptional(read(), (eof, exception) -> {
      if (exception != null) {
        currentBlock = ByteBuffers.EMPTY_BUFFER;
        reportDrop(BLOCK_SIZE, exception);
        return false;
      }
      blockScratch.flip();
      if (!blockScratch.hasRemaining()) {
        return false;
      }
      currentBlock = ByteBuffers.readOnly(blockScratch);
      return true;
    });
  }

  /**
   * @return eof
   */
  private CompletionStage<Boolean> read() {
    return file.read(blockScratch).thenCompose(bytesRead -> {
      if (bytesRead < 0) {
        return CompletableFuture.completedFuture(true);
      }
      return blockScratch.hasRemaining() ? read() : CompletableFuture.completedFuture(false);
    });
  }

  /**
   * Reports corruption to the monitor. The buffer must be updated to remove the dropped bytes prior
   * to invocation.
   *
   * @throws IOException
   */
  private void reportCorruption(final long bytes, final String reason) throws Exception {
    if (monitor != null) {
      monitor.corruption(bytes, reason);
    }
  }

  /**
   * Reports dropped bytes to the monitor. The buffer must be updated to remove the dropped bytes
   * prior to invocation.
   *
   * @throws IOException
   */
  private void reportDrop(final long bytes, final Throwable reason) throws Exception {
    if (monitor != null) {
      monitor.corruption(bytes, reason);
    }
  }

  @Override
  public CompletionStage<Void> asyncClose() {
    final CompletionStage<Void> f = file.asyncClose();
    ByteBuffers.freeDirect(blockScratch);
    recordScratch.close();
    return f;
  }
}
