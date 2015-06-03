
package org.iq80.leveldb.util;

import java.nio.ByteBuffer;

import org.iq80.leveldb.MemoryManager;

public class GrowingBuffer
{
    private final MemoryManager memory;
    private ByteBuffer buffer;

    public GrowingBuffer(final int initialSize, MemoryManager memory)
    {
        this.memory = memory;
        this.buffer = this.memory.allocate(nextPowerOf2(initialSize));
    }


    public GrowingBuffer put(ByteBuffer src)
    {
        final int deficit = src.remaining() - this.buffer.remaining();
        if (deficit > 0) {
            ByteBuffer oldBuffer = this.buffer;
            this.buffer = this.memory.allocate(nextPowerOf2(oldBuffer.capacity() + deficit));
            oldBuffer.flip();
            this.buffer.put(oldBuffer);
        }
        this.buffer.put(src);
        return this;
    }

    public int filled()
    {
        return this.buffer.position();
    }

    public void clear()
    {
        this.buffer.clear();
    }

    public ByteBuffer get()
    {
        this.buffer.flip();
        return this.buffer;
    }

    /**
     * inclusive, e.g. nextPowerOf2(1024) -> 1024
     */
    private static int nextPowerOf2(final int num)
    {
        return 1 << (Integer.SIZE - Integer.numberOfLeadingZeros(num - 1));
    }
}
