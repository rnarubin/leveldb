package org.iq80.leveldb;

import java.nio.ByteBuffer;

public interface MemoryManager
{
    ByteBuffer allocate(int capacity);

    void free(ByteBuffer buffer);
}
