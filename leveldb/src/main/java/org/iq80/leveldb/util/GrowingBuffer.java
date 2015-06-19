
package org.iq80.leveldb.util;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.iq80.leveldb.MemoryManager;
import org.iq80.leveldb.impl.InternalKey;

public class GrowingBuffer
        implements Closeable
{
    private final MemoryManager memory;
    private ByteBuffer buffer;
    private int oldpos;

    public GrowingBuffer(final int initialSize, MemoryManager memory)
    {
        this.memory = memory;
        this.buffer = this.memory.allocate(nextPowerOf2(initialSize));
        this.oldpos = this.buffer.position();
    }

    /**
     * note: returns internal ByteBuffer
     */
    public ByteBuffer ensureSpace(int length)
    {
        final int deficit = length - buffer.remaining();
        if (deficit > 0) {
            ByteBuffer oldBuffer = buffer;
            oldBuffer.limit(oldBuffer.position()).position(oldpos);
            buffer = memory.allocate(nextPowerOf2(oldBuffer.capacity() + deficit));
            oldpos = buffer.position();
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

    public GrowingBuffer writeLengthPrefixedKey(InternalKey key)
    {
        // 5 for max size of variable length int
        final int writeSize = key.getEncodedSize() + 5;
        ByteBuffer dst = ensureSpace(writeSize);
        VariableLengthQuantity.writeVariableLengthInt(writeSize, dst);
        key.writeToBuffer(dst);
        return this;
    }

    public int filled()
    {
        return buffer.position() - oldpos;
    }

    public void clear()
    {
        buffer.clear();
    }

    public ByteBuffer get()
    {
        return ByteBuffers.duplicate(buffer, oldpos, buffer.position());
    }

    /**
     * inclusive, e.g. nextPowerOf2(1024) -> 1024
     */
    private static int nextPowerOf2(final int num)
    {
        return 1 << (Integer.SIZE - Integer.numberOfLeadingZeros(num - 1));
    }

    @Override
    public String toString()
    {
        return "GrowingBuffer [buffer=" + buffer + "]";
    }

    @Override
    public void close()
            throws IOException
    {
        memory.free(buffer);
    }
}
