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
package org.iq80.leveldb.table;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.iq80.leveldb.Compression;
import org.iq80.leveldb.Env.RandomReadFile;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.EncodedInternalKey;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.SeekingIterable;
import org.iq80.leveldb.util.ByteBufferCrc32;
import org.iq80.leveldb.util.ByteBuffers;
import org.iq80.leveldb.util.CompletableFutures;
import org.iq80.leveldb.util.ReferenceCounted;
import org.iq80.leveldb.util.Snappy;

import com.google.common.base.Preconditions;

public final class Table extends ReferenceCounted<Table>
    implements SeekingIterable<InternalKey, ByteBuffer> {

  private final RandomReadFile file;
  private final Comparator<InternalKey> comparator;
  private final BlockHandle metaindexBlockHandle;
  private final Compression compression;
  private final boolean verifyChecksums;
  /**
   * this field really should be final, but initializing it requires a full block read which creates
   * very confusing code without a Table initialized. So don't mutate this outside of newTable!
   */
  private Block<InternalKey> indexBlock;

  private Table(final RandomReadFile file, final Comparator<InternalKey> comparator,
      final Options options, final BlockHandle metaindexBlockHandle) {
    Preconditions.checkNotNull(file, "file is null");
    Preconditions.checkNotNull(comparator, "comparator is null");

    this.verifyChecksums = options.verifyChecksums();
    this.compression = options.compression();

    this.file = file;
    this.comparator = comparator;

    this.metaindexBlockHandle = metaindexBlockHandle;
  }

  public static CompletionStage<Table> newTable(final RandomReadFile file,
      final Comparator<InternalKey> comparator, final Options options) {
    final long size = file.size();
    Preconditions.checkArgument(size >= Footer.ENCODED_LENGTH,
        "File is corrupt: size must be at least %s bytes", Footer.ENCODED_LENGTH);

    return file.read(size - Footer.ENCODED_LENGTH, Footer.ENCODED_LENGTH).thenCompose(footerBuf -> {
      final Footer footer = Footer.readFooter(footerBuf);
      final Table table = new Table(file, comparator, options, footer.getMetaindexBlockHandle());
      return table.readBlock(footer.getIndexBlockHandle()).thenApply(block -> {
        table.indexBlock = block;
        return table;
      });
    });
  }

  @Override
  public TableIterator iterator() {
    return new TableIterator(this, indexBlock.iterator());
  }

  public CompletionStage<Block<InternalKey>> openBlock(final ByteBuffer blockEntry) {
    return readBlock(BlockHandle.readBlockHandle(ByteBuffers.duplicate(blockEntry)));
  }

  private CompletionStage<Block<InternalKey>> readBlock(final BlockHandle blockHandle) {
    return file
        .read(blockHandle.getOffset(), blockHandle.getDataSize() + BlockTrailer.ENCODED_LENGTH)
        .thenCompose(readBuffer -> {
          Preconditions.checkState(
              readBuffer.remaining() == blockHandle.getDataSize() + BlockTrailer.ENCODED_LENGTH,
              "read buffer incorrect size (%d, %d)", readBuffer.remaining(),
              blockHandle.getDataSize() + BlockTrailer.ENCODED_LENGTH);

          final int dataStart = readBuffer.position();
          readBuffer.position(readBuffer.limit() - BlockTrailer.ENCODED_LENGTH);
          final BlockTrailer blockTrailer = BlockTrailer.readBlockTrailer(readBuffer);
          readBuffer.limit(readBuffer.limit() - BlockTrailer.ENCODED_LENGTH).position(dataStart);

          // only verify check sums if explicitly asked by the user
          if (verifyChecksums) {
            // checksum data and the compression type in the trailer
            final ByteBufferCrc32 checksum = ByteBuffers.crc32();
            checksum.update(readBuffer, dataStart, blockHandle.getDataSize() + 1);
            final int actualCrc32c = ByteBuffers.maskChecksum(checksum.getIntValue());

            if (blockTrailer.getCrc32c() != actualCrc32c) {
              return CompletableFutures.exceptionalFuture(new IOException(
                  String.format("Block corrupted: checksum mismatch in file %s", file.toString())));
            }
          }

          return CompletableFuture.completedFuture(new Block<InternalKey>(
              ByteBuffers
                  .readOnly(uncompressIfNecessary(readBuffer, blockTrailer.getCompressionId())),
              comparator, buffer -> new EncodedInternalKey(buffer)));
        });
  }

  private ByteBuffer uncompressIfNecessary(final ByteBuffer compressedData,
      final byte compressionId) {
    // TODO(postrelease) multiple live compressions
    if (compressionId == 0) {
      // not compressed
      return compressedData;
    }
    if (compression != null && compressionId == compression.persistentId()) {
      // id matches user compression
      return uncompress(compression, compressedData);
    }
    if (compressionId == 1) {
      // id matches Snappy, but not user compression, implying legacy data
      return uncompress(Snappy.instance(), compressedData);
    } else {
      throw new IllegalArgumentException("Unknown compression identifier: " + compressionId);
    }
  }

  private static ByteBuffer uncompress(final Compression compression,
      final ByteBuffer compressedData) {
    final ByteBuffer dst = ByteBuffer.allocate(compression.maxUncompressedLength(compressedData))
        .order(ByteOrder.LITTLE_ENDIAN);
    final int oldpos = dst.position();
    final int length = compression.uncompress(compressedData, dst);
    dst.limit(oldpos + length).position(oldpos);
    return dst;
  }

  /**
   * Given a key, return an approximate byte offset in the file where the data for that key begins
   * (or would begin if the key were present in the file). The returned value is in terms of file
   * bytes, and so includes effects like compression of the underlying data. For example, the
   * approximate offset of the last key in the table will be close to the file length.
   */
  public long getApproximateOffsetOf(final InternalKey key) {
    final BlockIterator<InternalKey> iterator = indexBlock.iterator();
    iterator.seek(key);
    if (iterator.hasNext()) {
      final BlockHandle blockHandle = BlockHandle.readBlockHandle(iterator.next().getValue());
      return blockHandle.getOffset();
    }

    // key is past the last key in the file. Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    return metaindexBlockHandle.getOffset();
  }

  @Override
  protected final Table getThis() {
    return this;
  }

  @Override
  protected final CompletionStage<Void> dispose() {
    return file.asyncClose();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("Table");
    sb.append("[file=").append(file);
    sb.append(", comparator=").append(comparator);
    sb.append(", verifyChecksums=").append(verifyChecksums);
    sb.append(']');
    return sb.toString();
  }

}
