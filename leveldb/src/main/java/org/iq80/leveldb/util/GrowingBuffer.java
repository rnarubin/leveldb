
package org.iq80.leveldb.util;

import java.io.Closeable;
import java.nio.ByteBuffer;

import org.iq80.leveldb.MemoryManager;
import org.iq80.leveldb.impl.InternalKey;

public class GrowingBuffer
        implements Closeable
{
    private final MemoryManager memory;
    private ByteBuffer buffer;
    private int startPos;

    public GrowingBuffer(final int initialSize, MemoryManager memory)
    {
        this.memory = memory;
        this.buffer = this.memory.allocate(initialSize);
        this.startPos = this.buffer.position();
    }

    /**
     * note: returns internal ByteBuffer
     */
    public ByteBuffer ensureSpace(int length)
    {
        final int deficit = length - buffer.remaining();
        if (deficit > 0) {
            ByteBuffer oldBuffer = buffer;
            buffer = memory.allocate(sizeUp(oldBuffer.limit() - startPos, deficit));
            oldBuffer.limit(oldBuffer.position()).position(startPos);
            startPos = buffer.position();
            buffer.put(oldBuffer);
            memory.free(oldBuffer);
        }
        return buffer;
    }

    public GrowingBuffer put(ByteBuffer src)
    {
        ensureSpace(src.remaining()).put(src);
        return this;
    }

    public GrowingBuffer put(byte[] src)
    {
        ensureSpace(src.length).put(src);
        return this;
    }

    public GrowingBuffer putInt(int val)
    {
        ensureSpace(SizeOf.SIZE_OF_INT).putInt(val);
        return this;
    }

    public GrowingBuffer putLong(long val)
    {
        ensureSpace(SizeOf.SIZE_OF_LONG).putLong(val);
        return this;
    }

    public GrowingBuffer writeLengthPrefixedKey(InternalKey key)
    {
        final int writeSize = key.getEncodedSize();
        // 5 for max size of variable length int
        ByteBuffer dst = ensureSpace(writeSize + 5);
        VariableLengthQuantity.writeVariableLengthInt(writeSize, dst);
        key.writeToBuffer(dst);
        return this;
    }

    public int filled()
    {
        return buffer.position() - startPos;
    }

    public void clear()
    {
        buffer.position(startPos);
    }

    public ByteBuffer get()
    {
        return ByteBuffers.duplicate(buffer, startPos, buffer.position());
    }

    private static int sizeUp(final int oldSize, final int deficit)
    {
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
    public String toString()
    {
        return "GrowingBuffer [buffer=" + buffer + "]";
    }

    @Override
    public void close()
    {
        memory.free(buffer);
    }
}
