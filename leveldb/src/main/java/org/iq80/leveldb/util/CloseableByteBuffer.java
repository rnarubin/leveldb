package org.iq80.leveldb.util;

import java.io.Closeable;
import java.nio.ByteBuffer;

public abstract class CloseableByteBuffer implements Closeable
{
   public final ByteBuffer buffer;

   protected CloseableByteBuffer(final ByteBuffer buffer)
   {
      this.buffer = buffer;
   }
}
