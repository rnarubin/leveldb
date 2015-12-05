
package com.cleversafe.leveldb.util;

import java.io.Closeable;
import java.nio.ByteBuffer;

import com.cleversafe.leveldb.impl.InternalKey;

public final class GrowingBuffer implements Closeable {
  private final MemoryManager memory;
  private ByteBuffer buffer;
  private int startPos;

  public GrowingBuffer(final int initialSize, final MemoryManager memory) {
    this.memory = memory;
    this.buffer = this.memory.allocate(initialSize);
    this.startPos = this.buffer.position();
  }

  /**
   * note: returns internal ByteBuffer
   */
  public ByteBuffer ensureSpace(final int length) {
    final int deficit = length - buffer.remaining();
    if (deficit > 0) {
      final ByteBuffer oldBuffer = buffer;
      buffer = memory.allocate(sizeUp(oldBuffer.limit() - startPos, deficit));
      oldBuffer.limit(oldBuffer.position()).position(startPos);
      startPos = buffer.position();
      buffer.put(oldBuffer);
      memory.free(oldBuffer);
    }
    return buffer;
  }

  public GrowingBuffer put(final ByteBuffer src) {
    ensureSpace(src.remaining()).put(src);
    return this;
  }

  public GrowingBuffer put(final byte[] src) {
    ensureSpace(src.length).put(src);
    return this;
  }

  public GrowingBuffer putInt(final int val) {
    ensureSpace(SizeOf.SIZE_OF_INT).putInt(val);
    return this;
  }

  public GrowingBuffer putLong(final long val) {
    ensureSpace(SizeOf.SIZE_OF_LONG).putLong(val);
    return this;
  }

  public GrowingBuffer writeLengthPrefixedKey(final InternalKey key) {
    final int writeSize = key.getEncodedSize();
    // 5 for max size of variable length int
    final ByteBuffer dst = ensureSpace(writeSize + 5);
    VariableLengthQuantity.writeVariableLengthInt(writeSize, dst);
    key.writeToBuffer(dst);
    return this;
  }

  public int filled() {
    return buffer.position() - startPos;
  }

  public void clear() {
    buffer.position(startPos);
  }

  public ByteBuffer get() {
    return ByteBuffers.duplicate(buffer, startPos, buffer.position());
  }

  private static int sizeUp(final int oldSize, final int deficit) {
    assert deficit > 0;
    final int minSize = oldSize + deficit;
    int newSize = oldSize + (oldSize >> 1);
    if (newSize < minSize) {
      newSize = Integer.highestOneBit(minSize - 1) << 1;
    }
    if (newSize < 0) {
      newSize = Integer.MAX_VALUE;
    }
    return newSize;
  }

  @Override
  public String toString() {
    return "GrowingBuffer [buffer=" + buffer + "]";
  }

  @Override
  public void close() {
    memory.free(buffer);
  }
}