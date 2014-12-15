package org.iq80.leveldb.util;

import java.util.concurrent.atomic.AtomicLong;

public abstract class ConcurrentNonCopyWriter<B extends CloseableByteBuffer>
{
   private final AtomicLong position = new AtomicLong(0);

   public B requestSpace(final int length)
   {
      return getBuffer(this.position.getAndAdd(length), length);
   }

   public B requestSpace(final LongToIntFunction getLength)
   {
      int length;
      long oldPosition;
      do
      {
         oldPosition = this.position.get();
      } while (!this.position.compareAndSet(oldPosition,
            length = getLength.applyAsInt(oldPosition)));
      return getBuffer(oldPosition, length);
   }

   protected abstract B getBuffer(long position, int length);

}
