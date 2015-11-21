package com.cleversafe.leveldb.table;

import static com.cleversafe.leveldb.impl.DbConstants.TARGET_FILE_SIZE;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cleversafe.leveldb.Compression;
import com.cleversafe.leveldb.Env.SequentialWriteFile;
import com.cleversafe.leveldb.impl.InternalKey;
import com.cleversafe.leveldb.impl.InternalKeyComparator;
import com.cleversafe.leveldb.util.ByteBufferCrc32C;
import com.cleversafe.leveldb.util.ByteBuffers;
import com.cleversafe.leveldb.util.MemoryManagers;


public class TableBuilder implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableBuilder.class);

  private final int blockRestartInterval;
  private final int blockSize;
  private final Compression compression;
  private final BlockBuilder dataBlockBuilder;
  private final BlockBuilder indexBlockBuilder;
  private final Comparator<InternalKey> internalKeyComparator;
  public final SequentialWriteFile file;

  private BlockHandle pendingHandle;
  private long position;
  private InternalKey lastKey;

  public TableBuilder(final int blockRestartInterval, final int blockSize,
      final Compression compression, final SequentialWriteFile file,
      final InternalKeyComparator internalKeyComparator) {
    this.file = file;
    this.internalKeyComparator = internalKeyComparator;

    this.blockRestartInterval = blockRestartInterval;
    this.blockSize = blockSize;
    this.compression = compression;

    this.dataBlockBuilder = new BlockBuilder((int) Math.min(blockSize * 1.1, TARGET_FILE_SIZE),
        blockRestartInterval, internalKeyComparator.getUserComparator(), MemoryManagers.direct());

    // with expected 50% compression
    final int expectedNumberOfBlocks = 1024;
    this.indexBlockBuilder =
        new BlockBuilder(BlockHandle.MAX_ENCODED_LENGTH * expectedNumberOfBlocks, 1,
            internalKeyComparator.getUserComparator(), MemoryManagers.direct());

    this.pendingHandle = null;
    this.position = 0;
    this.lastKey = null;
  }

  public long getFileSizeEstimate() {
    return position + dataBlockBuilder.currentSizeEstimate();
  }

  public CompletionStage<Void> add(final InternalKey key, final ByteBuffer value) {
    assert key != null;
    assert value != null;
    assert lastKey == null
        || internalKeyComparator.compare(key, lastKey) > 0 : "key must be greater than last key";

    // If we just wrote a block, we can now add the handle to index block
    // We do not emit the index entry for a block until we have seen the
    // first key for the next data block. This allows us to use shorter
    // keys in the index block. For example, consider a block boundary
    // between the keys "the quick brown fox" and "the who". We can use
    // "the r" as the key for the index block entry since it is >= all
    // entries in the first block and < all entries in subsequent
    // blocks.
    if (pendingHandle != null) {
      assert dataBlockBuilder
          .isEmpty() : "Table has a pending index entry but data block builder is not empty";
      indexBlockBuilder.addHandle(lastKey, key, pendingHandle);
      pendingHandle = null;
    }

    dataBlockBuilder.add(key, value);
    lastKey = key;

    final int estimatedBlockSize = dataBlockBuilder.currentSizeEstimate();
    return estimatedBlockSize >= blockSize ? writeBlock(dataBlockBuilder).thenAccept(handle -> {
      pendingHandle = handle;
      position = handle.getEndPosition();
    }) : CompletableFuture.completedFuture(null);
  }

  private CompletionStage<PositionedHandle> writeBlock(final BlockBuilder blockBuilder) {
    // close the block
    final ByteBuffer raw = blockBuilder.finish();
    ByteBuffer blockContents = raw;
    byte compressionId = 0;
    // attempt to compress the block
    if (compression != null) {
      // include space for packing the block trailer
      final ByteBuffer compressedContents =
          ByteBuffer.allocate(compression.maxCompressedLength(raw) + BlockTrailer.ENCODED_LENGTH)
              .order(ByteOrder.LITTLE_ENDIAN);

      try {
        final int compressedSize = compression.compress(ByteBuffers.duplicate(raw),
            ByteBuffers.duplicate(compressedContents));

        // Don't use the compressed data if compressed less than 12.5%,
        if (compressedSize < raw.remaining() - (raw.remaining() / 8)) {
          compressedContents.limit(compressedContents.position() + compressedSize);
          blockContents = compressedContents;
          compressionId = compression.persistentId();
        }
      } catch (final Exception e) {
        LOGGER.warn("Compression failed", e);
        // compression failed, so just store uncompressed form
        blockContents = raw;
        compressionId = 0;
      }
    }

    // create block trailer
    final BlockTrailer blockTrailer =
        new BlockTrailer(compressionId, crc32c(blockContents, compressionId));

    // trailer space was reserved either in compressed output or finished block
    blockContents.mark().position(blockContents.limit())
        .limit(blockContents.limit() + BlockTrailer.ENCODED_LENGTH);
    BlockTrailer.writeBlockTrailer(blockTrailer, blockContents);
    blockContents.reset();

    final int dataSize = blockContents.remaining() - BlockTrailer.ENCODED_LENGTH;

    // write data and trailer
    return file.write(blockContents).thenApply(position -> {
      blockBuilder.reset();
      return new PositionedHandle(position, dataSize);
    });
  }

  /**
   * @return final file size
   */
  public CompletionStage<Long> finish() {
    final CompletionStage<?> flush;

    if (dataBlockBuilder.isEmpty()) {
      if (pendingHandle != null) {
        indexBlockBuilder.addHandle(lastKey, null, pendingHandle);
      }
      flush = CompletableFuture.completedFuture(null);
    } else {
      assert pendingHandle == null;
      // flush current data block
      flush = writeBlock(dataBlockBuilder).thenAccept(handle -> {
        indexBlockBuilder.addHandle(lastKey, null, handle);
      });
    }

    // TODO filters in meta block
    // TODO(postrelease): Add stats and other meta blocks
    final BlockBuilder metaIndexBlockBuilder = new BlockBuilder(256, blockRestartInterval,
        BytewiseComparator.instance(), MemoryManagers.heap());
    final CompletionStage<? extends BlockHandle> metaIndexWrite =
        flush.thenCompose(voided -> writeBlock(metaIndexBlockBuilder)
            .whenComplete((h, e) -> metaIndexBlockBuilder.close()));

    return metaIndexWrite
        .thenCompose(metaIndexBlockHandle -> writeBlock(indexBlockBuilder)
            .thenApply(indexBlockHandle -> new Footer(metaIndexBlockHandle, indexBlockHandle)))
        .thenCompose(footer -> {
          final ByteBuffer footerEncoding =
              ByteBuffer.allocate(Footer.ENCODED_LENGTH).order(ByteOrder.LITTLE_ENDIAN);
          footerEncoding.mark();
          Footer.writeFooter(footer, footerEncoding).reset();
          // write footer
          return file.write(footerEncoding)
              .thenApply(writePosition -> writePosition + Footer.ENCODED_LENGTH);
        }).thenCompose(fileLength -> file.sync().thenApply(synced -> fileLength));
  }

  public void abandon() {}

  public static int crc32c(final ByteBuffer data, final byte compressionId) {
    final ByteBufferCrc32C crc32 = ByteBuffers.crc32c();
    crc32.update(data, data.position(), data.remaining());
    crc32.update(compressionId & 0xFF);
    return ByteBuffers.maskChecksum(crc32.getIntValue());
  }

  @Override
  public void close() {
    dataBlockBuilder.close();
    indexBlockBuilder.close();
  }

  private static final class PositionedHandle extends BlockHandle {
    PositionedHandle(final long offset, final int dataSize) {
      super(offset, dataSize);
    }

    public long getEndPosition() {
      return getOffset() + getDataSize() + BlockTrailer.ENCODED_LENGTH;
    }
  }
}
