package org.iq80.leveldb.table;


import static org.iq80.leveldb.impl.VersionSet.TARGET_FILE_SIZE;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.iq80.leveldb.Compression;
import org.iq80.leveldb.Env.SequentialWriteFile;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.InternalKeyComparator;
import org.iq80.leveldb.util.ByteBufferCrc32C;
import org.iq80.leveldb.util.ByteBuffers;
import org.iq80.leveldb.util.Closeables;
import org.iq80.leveldb.util.MemoryManagers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


public class TableBuilder implements Closeable {
  /**
   * TABLE_MAGIC_NUMBER was picked by running echo http://code.google.com/p/leveldb/ | sha1sum and
   * taking the leading 64 bits.
   */
  public static final long TABLE_MAGIC_NUMBER = 0xdb4775248b80fb57L;

  private static final Logger LOGGER = LoggerFactory.getLogger(TableBuilder.class);

  private final int blockRestartInterval;
  private final int blockSize;
  private final Compression compression;

  private final SequentialWriteFile file;
  private final BlockBuilder dataBlockBuilder;
  private final BlockBuilder indexBlockBuilder;
  private final InternalKeyComparator internalKeyComparator;

  public TableBuilder(final Options options, final SequentialWriteFile file,
      final InternalKeyComparator internalKeyComparator) {
    Preconditions.checkNotNull(options, "options is null");
    Preconditions.checkNotNull(file, "file is null");

    this.file = file;
    this.internalKeyComparator = internalKeyComparator;

    blockRestartInterval = options.blockRestartInterval();
    blockSize = options.blockSize();
    compression = options.compression();

    dataBlockBuilder = new BlockBuilder((int) Math.min(blockSize * 1.1, TARGET_FILE_SIZE),
        blockRestartInterval, internalKeyComparator.getUserComparator(), MemoryManagers.direct());

    // with expected 50% compression
    final int expectedNumberOfBlocks = 1024;
    indexBlockBuilder = new BlockBuilder(BlockHandle.MAX_ENCODED_LENGTH * expectedNumberOfBlocks, 1,
        internalKeyComparator.getUserComparator(), MemoryManagers.direct());
  }

  public BuilderState init() {
    return new BuilderState();
  }

  public CompletionStage<BuilderState> add(final InternalKey key, final ByteBuffer value,
      final BuilderState state) {
    assert key != null;
    assert value != null;
    assert state.isEmpty() || internalKeyComparator.compare(key,
        state.lastKey) > 0 : "key must be greater than last key";

    // If we just wrote a block, we can now add the handle to index block
    // We do not emit the index entry for a block until we have seen the
    // first key for the next data block. This allows us to use shorter
    // keys in the index block. For example, consider a block boundary
    // between the keys "the quick brown fox" and "the who". We can use
    // "the r" as the key for the index block entry since it is >= all
    // entries in the first block and < all entries in subsequent
    // blocks.
    if (state.pendingHandle != null) {
      assert dataBlockBuilder
          .isEmpty() : "Table has a pending index entry but data block builder is empty";
      indexBlockBuilder.addHandle(state.lastKey, key, state.pendingHandle);
    }

    dataBlockBuilder.add(key, value);

    final int estimatedBlockSize = dataBlockBuilder.currentSizeEstimate();
    return estimatedBlockSize >= blockSize
        ? writeBlock(dataBlockBuilder)
            .thenApply(handle -> new BuilderState(handle, handle.getEndPosition(), key))
        : CompletableFuture.completedFuture(new BuilderState(null, state.position, key));
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
  public CompletionStage<Long> finish(final BuilderState state) {
    final CompletionStage<?> flush;

    if (dataBlockBuilder.isEmpty()) {
      if (state.pendingHandle != null) {
        indexBlockBuilder.addHandle(state.lastKey, null, state.pendingHandle);
      }
      flush = CompletableFuture.completedFuture(null);
    } else {
      assert state.pendingHandle == null;
      // flush current data block
      flush = writeBlock(dataBlockBuilder).thenAccept(handle -> {
        indexBlockBuilder.addHandle(state.lastKey, null, handle);
      });
    }

    // TODO filters in meta block
    // TODO(postrelease): Add stats and other meta blocks
    final BlockBuilder metaIndexBlockBuilder = new BlockBuilder(256, blockRestartInterval,
        new BytewiseComparator(), MemoryManagers.heap());
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
        });
  }

  public void abandon() {}

  public static int crc32c(final ByteBuffer data, final byte compressionId) {
    final ByteBufferCrc32C crc32 = ByteBuffers.crc32c();
    crc32.update(data, data.position(), data.remaining());
    crc32.update(compressionId & 0xFF);
    return ByteBuffers.maskChecksum(crc32.getIntValue());
  }

  @Override
  public void close() throws IOException {
    Closeables.closeIO(dataBlockBuilder, indexBlockBuilder);
  }

  public final class BuilderState {
    private final BlockHandle pendingHandle;
    private final long position;
    private final boolean isEmpty;
    private final InternalKey lastKey;

    private BuilderState() {
      this.pendingHandle = null;
      this.position = 0;
      this.isEmpty = true;
      this.lastKey = null;
    }

    private BuilderState(final BlockHandle pendingHandle, final long position,
        final InternalKey lastKey) {
      this.pendingHandle = pendingHandle;
      this.position = position;
      this.lastKey = lastKey;
      this.isEmpty = false;
    }

    public long getFileSizeEstimate() {
      return position + dataBlockBuilder.currentSizeEstimate();
    }

    public boolean isEmpty() {
      return isEmpty;
    }
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
