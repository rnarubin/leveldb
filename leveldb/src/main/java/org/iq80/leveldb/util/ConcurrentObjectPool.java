package org.iq80.leveldb.util;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicIntegerArray;

import com.google.common.base.Supplier;
import com.google.common.primitives.UnsignedInteger;

public abstract class ConcurrentObjectPool<T> implements ObjectPool<T>
{
   /**
    * Returns a fixed-size object pool containing the given group of objects. If an object is
    * requested when none are available (based on a weak consistency view owing to concurrent
    * modification) the given allocator will be used to generate a new object to return to the
    * caller. This generated object will <i>not</i> be added to the object pool, and the
    * {@link ObjectPool#size() size} of the pool will be unchanged
    * @param objects set of objects used to populate the pool
    * @param allocator a {@link Supplier} used to generate a new object when none are available
    */
   public static <T> ObjectPool<T> fixedAllocatingPool(Iterable<T> objects, Supplier<T> allocator)
   {
      return new FixedAllocatingPool<>(objects, allocator);
   }
   
   /**
    * Returns a fixed-size object pool containing the given group of objects. If an object is
    * requested when none are available (based on a strong consistency view enforced by locking
    * mechanisms) the caller will block until an object is available.
    * @param objects set of objects used to populate the pool
    */
   public static <T> ObjectPool<T> fixedBlockingPool(Iterable<T> objects)
   {
      return new FixedBlockingPool<>(objects);
   }
   
   private static class FixedAllocatingPool<T> extends FixedBitSetPool<T>
   {
      private final Supplier<T> allocator;
      public FixedAllocatingPool(Iterable<T> objects, Supplier<T> allocator)
      {
         super(objects);
         this.allocator = allocator;
      }

      @Override
      public PooledObject<T> acquire()
      {
         PooledObject<T> ret = super.acquire();
         return ret != null ? ret : new SimplePooledObject<T>(allocator.get());
      }
   }
   
   private static class FixedBlockingPool<T> extends FixedBitSetPool<T>
   {
      private final Semaphore sem;
      public FixedBlockingPool(Iterable<T> objects)
      {
         super(objects);
         this.sem = new Semaphore(this.size());
      }
      
      @Override
      public PooledObject<T> acquire()
      {
         sem.acquireUninterruptibly();
         PooledObject<T> ret;
         // potential to miss a released object based on the initial probe position
         // the looping will terminate, however, because the semaphore ensures a caller could only
         // reach this position if at least one object is available
         while((ret = super.acquire()) == null);
         return ret;
      }
      
      @Override
      protected void release(IndexedPooledObject pooledObject)
      {
         super.release(pooledObject);
         sem.release();
      }
   }
   
   private static class FixedBitSetPool<T> implements ObjectPool<T>
   {
      private final AtomicIntegerArray bitSet;
      private final IndexedPooledObject pool[];
      private final int bitsPerSegment = Integer.SIZE;

      @SuppressWarnings("unchecked")
      private FixedBitSetPool(Iterable<T> objects)
      {
         int i = 0;
         final ArrayList<IndexedPooledObject> poolAsList = new ArrayList<>();
         for(T o:objects) {
            poolAsList.add(new IndexedPooledObject(i++, o));
         }

         if(poolAsList.size() < 1)
            throw new IllegalArgumentException("Fixed object pool must be given initial objects");

         this.pool = poolAsList.toArray((IndexedPooledObject[])Array.newInstance(IndexedPooledObject.class, poolAsList.size()));
         final int[] backing = new int[pool.length / bitsPerSegment + 1];
         Arrays.fill(backing, (1 << bitsPerSegment) - 1);
         backing[backing.length-1] = (1 << (pool.length % bitsPerSegment)) - 1;
         this.bitSet = new AtomicIntegerArray(backing);
      }
      
      public int size()
      {
         return pool.length;
      }
      
      private int probeIndex()
      {
         if(bitSet.length() == 1) {
            return 0;
         }
         return Thread.currentThread().hashCode() % bitSet.length();
      }
      
      @Override
      public PooledObject<T> acquire()
      {
         final int len = bitSet.length();
         int bitIndex = 0;
         int bitSetIndex = probeIndex() - 1;
         for(int i = 0; i < len; i++){
            bitSetIndex = (bitSetIndex + 1) % len;
            int state;
            do {
               state = bitSet.get(bitSetIndex);
               if((bitIndex = Integer.lowestOneBit(state)) == 0) {
                  //no objects found in this segment
                  //break from CAS
                  break;
               }
            } while(!bitSet.compareAndSet(bitSetIndex, state, state & (~bitIndex)));
         }
         if(bitIndex == 0) {
            //no objects found in any segment
            return null;
         }
         return pool[bitSetIndex*bitsPerSegment + Integer.numberOfTrailingZeros(bitIndex)];
      }
      
      protected void release(IndexedPooledObject pooledObject)
      {
         final int bitSetIndex = pooledObject.index / bitsPerSegment;
         final int bitIndex = pooledObject.index % bitsPerSegment;
         int state;
         do {
            state = bitSet.get(bitSetIndex);
         } while(!bitSet.compareAndSet(bitSetIndex, state, state | (1 << bitIndex)));
      }

      protected class IndexedPooledObject implements PooledObject<T>
      {
         protected final int index;
         private final T object;
         public IndexedPooledObject(int index, T object)
         {
            this.index = index;
            this.object = object;
         }
         
         @Override
         public void close()
         {
            release(this);
         }

         @Override
         public T get()
         {
            return object;
         }
      }
   }
}
