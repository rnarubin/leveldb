package org.iq80.leveldb.util;

import java.io.IOException;

public interface ConcurrentNonCopyWriter<B extends CloseableByteBuffer>
{
    public B requestSpace(final int length)
            throws IOException;

    public B requestSpace(final LongToIntFunction getLength)
            throws IOException;
}
