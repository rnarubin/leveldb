
package org.iq80.leveldb.util;

import org.iq80.leveldb.impl.ReverseSeekingIterator;

import java.util.Map.Entry;

public abstract class AbstractReverseSeekingIterator<K, V> implements ReverseSeekingIterator<K, V>
{
   private boolean rHasPeeked;
   private Entry<K, V> rPeekedElement;
   private boolean hasPeeked;
   private Entry<K, V> peekedElement;

   @Override
   public final void seekToFirst()
   {
      rHasPeeked = hasPeeked = false;
      rPeekedElement = peekedElement = null;
      seekToFirstInternal();
   }

   @Override
   public void seekToLast()
   {
      rHasPeeked = hasPeeked = false;
      rPeekedElement = peekedElement = null;
      seekToLastInternal();
   }

   @Override
   public final void seek(K targetKey)
   {
      rHasPeeked = hasPeeked = false;
      rPeekedElement = peekedElement = null;
      seekInternal(targetKey);
   }

   @Override
   public final boolean hasNext()
   {
      return hasPeeked || hasNextInternal();
   }

   @Override
   public final boolean hasPrev()
   {
      return rHasPeeked || hasPrevInternal();
   }

   @Override
   public final Entry<K, V> next()
   {
      hasPeeked = false;
      peekedElement = null;
      Entry<K, V> next = getNextElement();
      rHasPeeked = true;
      rPeekedElement = next;
      return next;
   }

   @Override
   public final Entry<K, V> prev()
   {
      rHasPeeked = false;
      rPeekedElement = null;
      Entry<K, V> prev = getPrevElement();
      hasPeeked = true;
      peekedElement = prev;
      return prev;
   }

   @Override
   public final Entry<K, V> peek()
   {
      if (!hasPeeked)
      {
         peekedElement = getNextElement();
         getPrevElement();
         hasPeeked = true;
      }
      return peekedElement;
   }

   @Override
   public final Entry<K, V> peekPrev()
   {
      if (!rHasPeeked)
      {
         rPeekedElement = getPrevElement();
         getNextElement(); // reset to original position
         rHasPeeked = true;
      }
      return rPeekedElement;
   }

   @Override
   public void remove()
   {
      throw new UnsupportedOperationException();
   }

   protected abstract void seekToLastInternal();
   protected abstract void seekToFirstInternal();
   protected abstract void seekInternal(K targetKey);
   protected abstract boolean hasNextInternal();
   protected abstract boolean hasPrevInternal();
   protected abstract Entry<K, V> getNextElement();
   protected abstract Entry<K, V> getPrevElement();
}
