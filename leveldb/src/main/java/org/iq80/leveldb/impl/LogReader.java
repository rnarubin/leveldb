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
package org.iq80.leveldb.impl;

import org.iq80.leveldb.MemoryManager;
import org.iq80.leveldb.util.ByteBuffers;
import org.iq80.leveldb.util.GrowingBuffer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.iq80.leveldb.impl.LogChunkType.BAD_CHUNK;
import static org.iq80.leveldb.impl.LogChunkType.EOF;
import static org.iq80.leveldb.impl.LogChunkType.UNKNOWN;
import static org.iq80.leveldb.impl.LogChunkType.ZERO_TYPE;
import static org.iq80.leveldb.impl.LogChunkType.getLogChunkTypeByPersistentId;
import static org.iq80.leveldb.impl.LogConstants.BLOCK_SIZE;
import static org.iq80.leveldb.impl.LogConstants.HEADER_SIZE;
import static org.iq80.leveldb.impl.Logs.getChunkChecksum;

public class LogReader
        implements Closeable
{
    private final FileChannel fileChannel;

    private final LogMonitor monitor;

    private final boolean verifyChecksums;

    /**
     * Offset at which to start looking for the first record to return
     */
    private final long initialOffset;

    /**
     * Have we read to the end of the file?
     */
    private boolean eof;

    /**
     * Offset of the last record returned by readRecord.
     */
    private long lastRecordOffset;

    /**
     * Offset of the first location past the end of buffer.
     */
    private long endOfBufferOffset;

    /**
     * Scratch buffer in which the next record is assembled.
     */
    private final GrowingBuffer recordScratch;

    /**
     * Scratch buffer for current block.  The currentBlock is sliced off the underlying buffer.
     */
    private ByteBuffer blockScratch;

    /**
     * The current block records are being read from.
     */
    private ByteBuffer currentBlock = ByteBuffers.EMPTY_BUFFER;

    /**
     * Current chunk which is sliced from the current block.
     */
    private ByteBuffer currentChunk = ByteBuffers.EMPTY_BUFFER;

    private final MemoryManager memory;

    public LogReader(FileChannel fileChannel,
            LogMonitor monitor,
            boolean verifyChecksums,
            long initialOffset,
            MemoryManager memory)
    {
        this.fileChannel = fileChannel;
        this.monitor = monitor;
        this.verifyChecksums = verifyChecksums;
        this.initialOffset = initialOffset;
        this.memory = memory;
        this.blockScratch = this.memory.allocate(BLOCK_SIZE);
        this.blockScratch.mark();
        this.recordScratch = new GrowingBuffer(BLOCK_SIZE, this.memory);
    }

    public long getLastRecordOffset()
    {
        return lastRecordOffset;
    }

    /**
     * Skips all blocks that are completely before "initial_offset_".
     * <p/>
     * Handles reporting corruption
     *
     * @return true on success.
     */
    private boolean skipToInitialBlock()
    {
        int offsetInBlock = (int) (initialOffset % BLOCK_SIZE);
        long blockStartLocation = initialOffset - offsetInBlock;

        // Don't search a block if we'd be in the trailer
        if (offsetInBlock > BLOCK_SIZE - 6) {
            blockStartLocation += BLOCK_SIZE;
        }

        endOfBufferOffset = blockStartLocation;

        // Skip to start of first block that can contain the initial record
        if (blockStartLocation > 0) {
            try {
                fileChannel.position(blockStartLocation);
            }
            catch (IOException e) {
                reportDrop(blockStartLocation, e);
                return false;
            }
        }

        return true;
    }

    public ByteBuffer readRecord()
    {
        recordScratch.clear();

        // advance to the first record, if we haven't already
        if (lastRecordOffset < initialOffset) {
            if (!skipToInitialBlock()) {
                return null;
            }
        }

        // Record offset of the logical record that we're reading
        long prospectiveRecordOffset = 0;

        boolean inFragmentedRecord = false;
        while (true) {
            long physicalRecordOffset = endOfBufferOffset - currentChunk.remaining();
            LogChunkType chunkType = readNextChunk();
            switch (chunkType) {
                case FULL:
                    if (inFragmentedRecord) {
                        reportCorruption(recordScratch.filled(), "Partial record without end");
                        // simply return this full block
                    }
                    recordScratch.clear();
                    prospectiveRecordOffset = physicalRecordOffset;
                    lastRecordOffset = prospectiveRecordOffset;
                    return ByteBuffers.copy(currentChunk, memory);

                case FIRST:
                    if (inFragmentedRecord) {
                        reportCorruption(recordScratch.filled(), "Partial record without end");
                        // clear the scratch and start over from this chunk
                        recordScratch.clear();
                    }
                    prospectiveRecordOffset = physicalRecordOffset;
                    recordScratch.put(currentChunk);
                    inFragmentedRecord = true;
                    break;

                case MIDDLE:
                    if (!inFragmentedRecord) {
                        reportCorruption(recordScratch.filled(), "Missing start of fragmented record");

                        // clear the scratch and skip this chunk
                        recordScratch.clear();
                    }
                    else {
                        recordScratch.put(currentChunk);
                    }
                    break;

                case LAST:
                    if (!inFragmentedRecord) {
                        reportCorruption(recordScratch.filled(), "Missing start of fragmented record");

                        // clear the scratch and skip this chunk
                        recordScratch.clear();
                    }
                    else {
                        recordScratch.put(currentChunk);
                        lastRecordOffset = prospectiveRecordOffset;
                        // TODO check this copy
                        return ByteBuffers.copy(recordScratch.get(), memory);
                    }
                    break;

                case EOF:
                    if (inFragmentedRecord) {
                        reportCorruption(recordScratch.filled(), "Partial record without end");

                        // clear the scratch and return
                        recordScratch.clear();
                    }
                    return null;

                case BAD_CHUNK:
                    if (inFragmentedRecord) {
                        reportCorruption(recordScratch.filled(), "Error in middle of record");
                        inFragmentedRecord = false;
                        recordScratch.clear();
                    }
                    break;

                default:
                    int dropSize = currentChunk.remaining();
                    if (inFragmentedRecord) {
                        dropSize += recordScratch.filled();
                    }
                    reportCorruption(dropSize, String.format("Unexpected chunk type %s", chunkType));
                    inFragmentedRecord = false;
                    recordScratch.clear();
                    break;
            }
        }
    }

    /**
     * Return type, or one of the preceding special values
     */
    private LogChunkType readNextChunk()
    {
        // clear the current chunk
        currentChunk = ByteBuffers.EMPTY_BUFFER;

        // read the next block if necessary
        if (currentBlock.remaining() < HEADER_SIZE) {
            if (!readNextBlock()) {
                if (eof) {
                    return EOF;
                }
            }
        }

        // parse header
        int expectedChecksum = currentBlock.getInt();
        int length = ByteBuffers.readUnsignedByte(currentBlock);
        length = length | ByteBuffers.readUnsignedByte(currentBlock) << 8;
        byte chunkTypeId = currentBlock.get();
        LogChunkType chunkType = getLogChunkTypeByPersistentId(chunkTypeId);

        // verify length
        if (length > currentBlock.remaining()) {
            int dropSize = currentBlock.remaining() + HEADER_SIZE;
            reportCorruption(dropSize, "Invalid chunk length");
            memory.free(currentBlock);
            currentBlock = ByteBuffers.EMPTY_BUFFER;
            return BAD_CHUNK;
        }

        // skip zero length records
        if (chunkType == ZERO_TYPE && length == 0) {
            // Skip zero length record without reporting any drops since
            // such records are produced by the writing code.
            memory.free(currentBlock);
            currentBlock = ByteBuffers.EMPTY_BUFFER;
            return BAD_CHUNK;
        }

        // Skip physical record that started before initialOffset
        if (endOfBufferOffset - HEADER_SIZE - length < initialOffset) {
            currentBlock.position(currentBlock.position() + length);
            return BAD_CHUNK;
        }

        // read the chunk
        currentChunk = ByteBuffers.duplicateAndAdvance(currentBlock, length);

        if (verifyChecksums) {
            int actualChecksum = getChunkChecksum(chunkTypeId, currentChunk);
            if (actualChecksum != expectedChecksum) {
                // Drop the rest of the buffer since "length" itself may have
                // been corrupted and if we trust it, we could find some
                // fragment of a real log record that just happens to look
                // like a valid log record.
                int dropSize = currentBlock.remaining() + HEADER_SIZE;
                memory.free(currentBlock);
                currentBlock = ByteBuffers.EMPTY_BUFFER;
                reportCorruption(dropSize, "Invalid chunk checksum");
                return BAD_CHUNK;
            }
        }

        // Skip unknown chunk types
        // Since this comes last so we the, know it is a valid chunk, and is just a type we don't understand
        if (chunkType == UNKNOWN) {
            reportCorruption(length, String.format("Unknown chunk type %d", chunkType.getPersistentId()));
            return BAD_CHUNK;
        }

        return chunkType;
    }

    public boolean readNextBlock()
    {
        if (eof) {
            return false;
        }

        // clear the block
        blockScratch.reset();

        // read the next full block
        while (blockScratch.hasRemaining()) {
            try {
                int bytesRead = fileChannel.read(blockScratch);
                if (bytesRead < 0) {
                    // no more bytes to read
                    eof = true;
                    break;
                }
                endOfBufferOffset += bytesRead;
            }
            catch (IOException e) {
                memory.free(currentBlock);
                currentBlock = ByteBuffers.EMPTY_BUFFER;
                reportDrop(BLOCK_SIZE, e);
                eof = true;
                return false;
            }

        }
        blockScratch.limit(blockScratch.position()).reset();
        // TODO check this copy
        if (currentBlock != ByteBuffers.EMPTY_BUFFER) {
            memory.free(currentBlock);
        }
        currentBlock = blockScratch;// ByteBuffers.copy(blockScratch, this.memory);
        blockScratch = memory.allocate(BLOCK_SIZE);
        blockScratch.mark();
        return currentBlock.hasRemaining();
    }

    /**
     * Reports corruption to the monitor.
     * The buffer must be updated to remove the dropped bytes prior to invocation.
     */
    private void reportCorruption(long bytes, String reason)
    {
        if (monitor != null) {
            monitor.corruption(bytes, reason);
        }
    }

    /**
     * Reports dropped bytes to the monitor.
     * The buffer must be updated to remove the dropped bytes prior to invocation.
     */
    private void reportDrop(long bytes, Throwable reason)
    {
        if (monitor != null) {
            monitor.corruption(bytes, reason);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        recordScratch.close();
        memory.free(blockScratch);
        if (currentBlock != ByteBuffers.EMPTY_BUFFER) {
            memory.free(currentBlock);
        }
        fileChannel.close();
    }
}
