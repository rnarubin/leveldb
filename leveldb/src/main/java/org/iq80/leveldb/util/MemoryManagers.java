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

    // if the user has set the default to be DIRECT via system properties, make it direct.
    // otherwise, the default was set to be:
    // 1) HEAP, so heap
    // 2) null, i.e. not set, so default to heap
    // 3) user configured: in which case default to heap in case it's null in sanitize check
    private static final MemoryManager DEFAULT = Options.make().memoryManager() == DIRECT ? DIRECT : HEAP;

    public static MemoryManager sanitize(final MemoryManager userManager)
    {
        // bypass sanitation for known defaults
        return userManager == null ? DEFAULT : userManager == HEAP ? HEAP : userManager == DIRECT ? DIRECT
                : new MemoryManager()
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
