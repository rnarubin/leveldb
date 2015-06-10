package org.iq80.leveldb.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.iq80.leveldb.MemoryManager;

public class MemoryManagers
{
    private MemoryManagers()
    {
    }

    private enum Heap
            implements MemoryManager
    {
        INSTANCE;
        @Override
        public ByteBuffer allocate(int capacity)
        {
            return ByteBuffer.allocate(capacity).order(ByteOrder.LITTLE_ENDIAN);
        }

        @Override
        public void free(ByteBuffer buffer)
        {
            // noop
        }
    }

    public static MemoryManager heap()
    {
        return Heap.INSTANCE;
    }

    /**
     * not intended for external use as naive allocations are generally expensive. useful for testing however
     */
    static MemoryManager direct()
    {
        return Direct.INSTANCE;
    }

    private enum Direct
            implements MemoryManager
    {
        INSTANCE;

        @Override
        public ByteBuffer allocate(int capacity)
        {
            return ByteBuffer.allocateDirect(capacity).order(ByteOrder.LITTLE_ENDIAN);
        }

        @Override
        public void free(ByteBuffer buffer)
        {
            ByteBuffers.freeDirect(buffer);
        }

    }

    public static MemoryManager sanitize(final MemoryManager userManager)
    {
        return userManager == null ? heap() : new MemoryManager()
        {
            @Override
            public ByteBuffer allocate(int capacity)
            {
                ByteBuffer ret = userManager.allocate(capacity).order(ByteOrder.LITTLE_ENDIAN);
                // for simplicity's sake, make sure we get transparent buffers
                return ret.position() == 0 && ret.limit() == ret.capacity() ? ret : ret.slice();
            }

            @Override
            public void free(ByteBuffer buffer)
            {
                userManager.free(buffer);
            }
        };
    }
}
