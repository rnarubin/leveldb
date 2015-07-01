package org.iq80.leveldb.util;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.iq80.leveldb.MemoryManager;

public class MemoryManagers
{
    private MemoryManagers()
    {
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

    /**
     * simple standard lib direction allocation. generally performs poorly for naive allocations,
     * not recommended for production use
     */
    public static MemoryManager direct()
    {
        return Direct.INSTANCE;
    }

    private enum Heap
            implements MemoryManager
    {
        INSTANCE;
        @Override
        public ByteBuffer allocate(int size)
        {
            return ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
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
}
