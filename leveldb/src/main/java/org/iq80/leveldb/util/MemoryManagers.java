package org.iq80.leveldb.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.iq80.leveldb.MemoryManager;

public enum MemoryManagers
        implements MemoryManagerFactory
{
    HEAP
    {
        @Override
        public MemoryManager make()
        {
            return SingleHeap.INSTANCE;
        }
    },

    DIRECT
    {
        @Override
        public MemoryManager make()
        {
            return null;
        }
    };

    private enum SingleHeap
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

    public static MemoryManager sanitize(final MemoryManager userManager)
    {
        // bypass sanitation for known defaults
        return userManager == null ? HEAP.make() : new MemoryManager()
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
