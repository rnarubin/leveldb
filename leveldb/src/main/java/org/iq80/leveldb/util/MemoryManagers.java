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

    public static MemoryManager sanitize(final MemoryManager userManager)
    {
        return userManager == null ? heap() : new MemoryManager()
        {
            @Override
            public ByteBuffer allocate(int capacity)
            {
                return userManager.allocate(capacity).order(ByteOrder.LITTLE_ENDIAN);
            }

            @Override
            public void free(ByteBuffer buffer)
            {
                userManager.free(buffer);
            }
        };
    }
}
