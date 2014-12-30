package org.iq80.leveldb.util;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.iq80.leveldb.util.ObjectPool.PooledObject;

import com.google.common.base.Supplier;

public abstract class ObjectPools
{
   /**
    * Returns a fixed-size, concurrent object pool containing the given group of objects. If an
    * object is requested when none are available (based on a weak consistency view owing to
    * concurrent modification) the given allocator will be used to generate a new object to return
    * to the caller. This generated object will <i>not</i> be added to the object pool, and the
    * {@link ObjectPool#size() size} of the pool will be unchanged
    * 
    * @param objects
    *           set of objects used to populate the pool
    * @param allocator
    *           a {@link Supplier} used to generate a new object when none are available
    */
   public static <T> ObjectPool<T> fixedAllocatingPool(Iterable<T> objects, Supplier<T> allocator)
   {
      return new FixedAllocatingPool<>(objects, allocator);
   }
   
   /**
    * Returns a fixed-size, concurrent object pool containing the given group of objects. If an
    * object is requested when none are available (based on a strong consistency view enforced by
    * locking mechanisms) the caller will block until an object is available.
    * 
    * @param objects
    *           set of objects used to populate the pool
    */
   public static <T> ObjectPool<T> fixedBlockingPool(Iterable<T> objects)
   {
      return new FixedBlockingPool<>(objects);
   }
   
   /**
    * Returns a {@link #fixedAllocatingPool} containing {@code num} ByteBuffers of {@code size}
    * capacity, allocated {@link ByteBuffer#allocateDirect(int) directly}. If the pool has no
    * available buffers when a caller attempts to {@link ObjectPool#acquire acquire}, a non-direct buffer will be
    * allocated and returned.
    * 
    * @param num
    *           the total number of buffers
    * @param size
    *           the size of each buffer in bytes
    */
   public static ObjectPool<ByteBuffer> directBufferPool(final int num, final int size)
   {
          ByteBuffer cache = ByteBuffer.allocateDirect(num * size);
          ArrayList<ByteBuffer> bufs = new ArrayList<>(num);
          for(int i = 0, pos = 0; i < num; i++, pos += size){
             cache.position(pos);
             cache.limit(pos + size);
             ByteBuffer subset = cache.slice();
             bufs.add(subset);
          }
          return new DirectBufferPool(bufs, new Supplier<ByteBuffer>()
          {
              @Override
              public ByteBuffer get()
              {
                 return ByteBuffer.allocate(size);
              }
          });
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
         PooledObject<T> ret = super.tryAcquire();
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
         while((ret = super.tryAcquire()) == null);
         return ret;
      }
      
      @Override
      public PooledObject<T> tryAcquire()
      {
         if(sem.tryAcquire())
         {
            PooledObject<T> ret;
            while((ret = super.tryAcquire()) == null);
            return ret;
         }
         return null;
      }
      
      @Override
      protected void release(IndexedPooledObject pooledObject)
      {
         super.release(pooledObject);
         sem.release();
      }
   }
   
   private static class DirectBufferPool extends FixedAllocatingPool<ByteBuffer>
   {
      public DirectBufferPool(Iterable<ByteBuffer> objects, Supplier<ByteBuffer> allocator)
      {
         super(objects, allocator);
      }
      @Override
      protected void release(IndexedPooledObject pooledBuffer)
      {
         pooledBuffer.get().clear();
         super.release(pooledBuffer);
      }
   }
   
   private abstract static class FixedBitSetPool<T> implements ObjectPool<T>
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
         final int[] backing = new int[(pool.length / bitsPerSegment) + ((pool.length%bitsPerSegment)==0?0:1)];
         Arrays.fill(backing, (int)((1L << bitsPerSegment) - 1));
         if(pool.length%bitsPerSegment!=0)
            backing[backing.length-1] = (int)((1L << (pool.length % bitsPerSegment)) - 1);
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
         return ThreadLocalRandom.current().nextInt(bitSet.length());
      }
      
      public abstract PooledObject<T> acquire();
      
      @Override
      public PooledObject<T> tryAcquire()
      {
         final int len = bitSet.length();
         int bitIndex = 0;
         int bitSetIndex = probeIndex() - 1;
         segments: for(int i = 0; i < len; i++){
            bitSetIndex = (bitSetIndex + 1) % len;
            int state;
            do {
               state = bitSet.get(bitSetIndex);
               if((bitIndex = Integer.lowestOneBit(state)) == 0) {
                  //no objects found in this segment
                  //break from CAS
                  continue segments;
               }
            } while(!bitSet.compareAndSet(bitSetIndex, state, state & (~bitIndex)));
            //CAS success
            return pool[bitSetIndex*bitsPerSegment + Integer.numberOfTrailingZeros(bitIndex)];
         }
         //no objects found in any segment
         return null;
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
   
   private static class SimplePooledObject<T> implements PooledObject<T>
   {
      private final T object;
      public SimplePooledObject(T object)
      {
         this.object = object;
      }
      
      @Override
      public void close(){} //noop
      
      @Override
      public T get()
      {
         return object;
      }
   }
}
