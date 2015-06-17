
package org.iq80.leveldb.util;

import java.nio.ByteBuffer;

import org.iq80.leveldb.MemoryManager;

public class GrowingBuffer
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
        final int deficit = length - this.buffer.remaining();
        if (deficit > 0) {
            ByteBuffer oldBuffer = this.buffer;
            oldBuffer.limit(oldBuffer.position()).position(this.oldpos);
            this.buffer = this.memory.allocate(nextPowerOf2(oldBuffer.capacity() + deficit));
            this.oldpos = this.buffer.position();
            this.buffer.put(oldBuffer);
        }
        return this.buffer;
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

    public int filled()
    {
        return this.buffer.position() - this.oldpos;
    }

    public void clear()
    {
        this.buffer.clear();
    }

    public ByteBuffer get()
    {
        return ByteBuffers.duplicate(this.buffer, this.oldpos, this.buffer.position());
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
}
