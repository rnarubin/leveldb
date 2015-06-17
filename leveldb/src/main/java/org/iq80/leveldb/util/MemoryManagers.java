package org.iq80.leveldb.util;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.iq80.leveldb.MemoryManager;

import com.google.common.base.FinalizablePhantomReference;
import com.google.common.base.FinalizableReferenceQueue;
import com.google.common.base.Throwables;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Sets;

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

    /**
     * strict buffer management checking for testing
     */
    static StrictMemoryManager strict()
    {
        return new StrictMemoryManager();
    }

    private static class StrictMemoryManager
            implements MemoryManager, Closeable
    {
        private final FinalizableReferenceQueue phantomQueue = new FinalizableReferenceQueue();
        private final Set<FinalizablePhantomReference<ByteBuffer>> refSet = Sets.newConcurrentHashSet();
        private final Map<ByteBuffer, AtomicBoolean> bufMap = new MapMaker().weakKeys().makeMap();
        private volatile Throwable backgroundException = null;
        private static final Field hb;
        static {
            try {
                hb = ByteBuffer.class.getDeclaredField("hb");
            }
            catch (NoSuchFieldException | SecurityException e) {
                throw new Error(e);
            }
            hb.setAccessible(true);
        }

        @Override
        public ByteBuffer allocate(int capacity)
        {
            final ByteBuffer buf = ByteBuffer.allocate(capacity).order(ByteOrder.LITTLE_ENDIAN);
            final AtomicBoolean freed = new AtomicBoolean(false);
            bufMap.put(buf, freed);
            refSet.add(new FinalizablePhantomReference<ByteBuffer>(buf, phantomQueue)
            {
                @Override
                public void finalizeReferent()
                {
                    refSet.remove(this);
                    if(!freed.get())
                    {
                        backgroundException = new IllegalStateException("buffer GC without free");
                    }
                }
            });
            return buf;
        }

        @Override
        public void free(ByteBuffer buffer)
        {
            AtomicBoolean freed = bufMap.get(buffer);
            if (freed == null) {
                throw new IllegalStateException("free called on buffer from foreign source");
            }
            if (!freed.compareAndSet(false, true)) {
                throw new IllegalStateException("double free");
            }
            try {
                // force NPE on use-after-free
                hb.set(buffer, null);
            }
            catch (IllegalArgumentException | IllegalAccessException e) {
                throw new Error(e);
            }
        }

        @Override
        public void close()
                throws IOException
        {
            phantomQueue.close();
            if (backgroundException != null) {
                Throwables.propagate(backgroundException);
            }
        }

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
