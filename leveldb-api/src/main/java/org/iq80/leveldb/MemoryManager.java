package org.iq80.leveldb;

import java.nio.ByteBuffer;

public interface MemoryManager
        extends Deallocator
{
    /**
     * Return a {@link ByteBuffer} with at least {@code size} remaining space.
     * The returned buffer must be {@link java.nio.ByteOrder#LITTLE_ENDIAN LITTLE_ENDIAN}.
     * This method must operate in a thread-safe manner as it may be called from many
     * threads concurrently; furthermore, the returned buffer may be {@link MemoryManager#free freed}
     * from a different thread than the one which called {@code allocate}
     */
    ByteBuffer allocate(int size);
}
