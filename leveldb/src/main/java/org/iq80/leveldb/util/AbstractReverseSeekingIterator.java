
package org.iq80.leveldb.util;

import org.iq80.leveldb.impl.ReverseSeekingIterator;

import com.google.common.base.Function;

import java.security.NoSuchAlgorithmException;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;

public abstract class AbstractReverseSeekingIterator<K, V> implements ReverseSeekingIterator<K, V>
{
   private Entry<K, V> rPeekedElement;
   private Entry<K, V> peekedElement;

   @Override
   public final void seekToFirst()
   {
      rPeekedElement = peekedElement = null;
      seekToFirstInternal();
   }

   @Override
   public void seekToLast()
   {
      rPeekedElement = peekedElement = null;
      seekToLastInternal();
   }

   @Override
   public final void seek(K targetKey)
   {
      rPeekedElement = peekedElement = null;
      seekInternal(targetKey);
   }

   @Override
   public final boolean hasNext()
   {
      return peekedElement != null || hasNextInternal();
   }

   @Override
   public final boolean hasPrev()
   {
      return rPeekedElement != null || hasPrevInternal();
   }

   @Override
   public final Entry<K, V> next()
   {
      peekedElement = null;
      Entry<K, V> next = getNextElement();
      if(next == null){
         throw new NoSuchElementException();
      }
      rPeekedElement = next;
      return next;
   }

   @Override
   public final Entry<K, V> prev()
   {
      rPeekedElement = null;
      Entry<K, V> prev = getPrevElement();
      if(prev == null){
         throw new NoSuchElementException();
      }
      peekedElement = prev;
      return prev;
   }

   @Override
   public final Entry<K, V> peek()
   {
      if (peekedElement == null)
      {
         peekedElement = getNextElement();
         if(peekedElement == null){
            throw new NoSuchElementException();
         }
         getPrevElement(); //reset to original position
      }
      return peekedElement;
   }

   @Override
   public final Entry<K, V> peekPrev()
   {
      if (rPeekedElement == null){
         rPeekedElement = getPrevElement();
         if(rPeekedElement == null){
            throw new NoSuchElementException();
         }
         getNextElement(); // reset to original position
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