package org.iq80.leveldb.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.iq80.leveldb.MemoryManager;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.util.ByteBuffers;

public enum MemoryManagers
        implements MemoryManager
{
    HEAP
    {
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
    },

    DIRECT
    {
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
    };

    private static final MemoryManager DEFAULT = Options.make().memoryManager();

    public static MemoryManager sanitze(final MemoryManager userManager)
    {
        return userManager == null ? DEFAULT : new MemoryManager()
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
