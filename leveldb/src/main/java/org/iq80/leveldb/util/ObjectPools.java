/*
 * Copyright (C) 2011 the original author or authors. See the notice.md file distributed with this
 * work for additional information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.iq80.leveldb.util;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.Supplier;

import org.iq80.leveldb.Deallocator;
import org.iq80.leveldb.MemoryManager;
import org.iq80.leveldb.util.ObjectPool.PooledObject;

public final class ObjectPools {
  private ObjectPools() {};

  /**
   * Returns a fixed-size, concurrent object pool containing the given group of objects. If an
   * object is requested when none are available (based on a weak consistency view owing to
   * concurrent modification) the given allocator will be used to generate a new object to return to
   * the caller. This generated object will <i>not</i> be added to the object pool, and the
   * {@link ObjectPool#size() size} of the pool will be unchanged
   *
   * @param objects set of objects used to populate the pool
   * @param allocator a {@link Supplier} used to generate a new object when none are available
   */
  public static <T> ObjectPool<T> fixedAllocatingPool(final Iterable<T> objects,
      final Supplier<T> allocator) {
    return new FixedAllocatingPool<>(objects, allocator);
  }

  public static <T> ObjectPool<T> fixedAllocatingPool(final Iterable<T> objects,
      final Supplier<T> allocator, final int bitsPerSegment) {
    return new FixedAllocatingPool<>(objects, allocator, bitsPerSegment);
  }

  /**
   * Returns a fixed-size, concurrent object pool containing the given group of objects. If an
   * object is requested when none are available (based on a strong consistency view enforced by
   * locking mechanisms) the caller will block until an object is available.
   *
   * @param objects set of objects used to populate the pool
   */
  public static <T> ObjectPool<T> fixedBlockingPool(final Iterable<T> objects) {
    return new FixedBlockingPool<>(objects);
  }

  /**
   * Returns a {@link #fixedAllocatingPool} containing {@code num} ByteBuffers of {@code size}
   * capacity, allocated {@link ByteBuffer#allocateDirect(int) directly}. If the pool has no
   * available buffers when a caller attempts to {@link ObjectPool#acquire acquire}, a non-direct
   * buffer will be allocated and returned.
   *
   * @param num the total number of buffers
   * @param size the size of each buffer in bytes
   */
  public static DirectBufferPool directBufferPool(final int num, final int size,
      final MemoryManager memory, final int bitsPerSegment) {
    final ByteBuffer cache = memory.allocate(num * size);
    final ArrayList<ByteBuffer> bufs = new ArrayList<>(num);
    for (int i = 0, pos = 0; i < num; i++, pos += size) {
      cache.position(pos);
      cache.limit(pos + size);
      final ByteBuffer subset = cache.slice();
      bufs.add(subset);
    }
    return new DirectBufferPool(cache, memory, bufs, () -> ByteBuffer.allocate(size),
        bitsPerSegment);
  }

  private static class FixedAllocatingPool<T> extends FixedBitSetPool<T> {
    private final Supplier<T> allocator;

    public FixedAllocatingPool(final Iterable<T> objects, final Supplier<T> allocator) {
      this(objects, allocator, Integer.SIZE);
    }

    public FixedAllocatingPool(final Iterable<T> objects, final Supplier<T> allocator,
        final int bitsPerSegment) {
      super(objects, bitsPerSegment);
      this.allocator = allocator;
    }

    @Override
    public PooledObject<T> acquire() {
      final PooledObject<T> ret = super.tryAcquire();
      return ret != null ? ret : new SimplePooledObject<T>(allocator.get());
    }
  }

  private static class FixedBlockingPool<T> extends FixedBitSetPool<T> {
    private final Semaphore sem;

    public FixedBlockingPool(final Iterable<T> objects) {
      super(objects);
      this.sem = new Semaphore(this.size());
    }

    @Override
    public PooledObject<T> acquire() {
      sem.acquireUninterruptibly();
      PooledObject<T> ret;
      // potential to miss a released object based on the initial probe position
      // the looping will terminate, however, because the semaphore ensures a caller could only
      // reach this position if at least one object is available
      while ((ret = super.tryAcquire()) == null) {
        ;
      }
      return ret;
    }

    @Override
    public PooledObject<T> tryAcquire() {
      if (sem.tryAcquire()) {
        PooledObject<T> ret;
        while ((ret = super.tryAcquire()) == null) {
          ;
        }
        return ret;
      }
      return null;
    }

    @Override
    protected void release(final IndexedPooledObject pooledObject) {
      super.release(pooledObject);
      sem.release();
    }
  }

  public final static class DirectBufferPool extends FixedAllocatingPool<ByteBuffer>
      implements AutoCloseable {
    private final ByteBuffer backingBuf;
    private final Deallocator deallocator;

    private DirectBufferPool(final ByteBuffer backingBuf, final Deallocator deallocator,
        final Iterable<ByteBuffer> objects, final Supplier<ByteBuffer> allocator,
        final int bitsPerSegment) {
      super(objects, allocator, bitsPerSegment);
      this.backingBuf = backingBuf;
      this.deallocator = deallocator;
    }

    @Override
    protected void release(final IndexedPooledObject pooledBuffer) {
      pooledBuffer.get().clear();
      super.release(pooledBuffer);
    }

    @Override
    public void close() {
      deallocator.free(backingBuf);
    }
  }

  private abstract static class FixedBitSetPool<T> implements ObjectPool<T> {
    private final AtomicIntegerArray bitSet;
    private final IndexedPooledObject pool[];
    private final int bitsPerSegment;

    private FixedBitSetPool(final Iterable<T> objects) {
      this(objects, Integer.SIZE);
    }

    @SuppressWarnings("unchecked")
    private FixedBitSetPool(final Iterable<T> objects, final int bitsPerSegment) {
      int i = 0;
      final ArrayList<IndexedPooledObject> poolAsList = new ArrayList<>();
      for (final T o : objects) {
        poolAsList.add(new IndexedPooledObject(i++, o));
      }

      if (poolAsList.size() < 1) {
        throw new IllegalArgumentException("Fixed object pool must be given initial objects");
      }

      this.pool = poolAsList.toArray(
          (IndexedPooledObject[]) Array.newInstance(IndexedPooledObject.class, poolAsList.size()));
      final int[] backing =
          new int[(pool.length / bitsPerSegment) + ((pool.length % bitsPerSegment) == 0 ? 0 : 1)];
      Arrays.fill(backing, (int) ((1L << bitsPerSegment) - 1));
      if (pool.length % bitsPerSegment != 0) {
        backing[backing.length - 1] = (int) ((1L << (pool.length % bitsPerSegment)) - 1);
      }
      this.bitSet = new AtomicIntegerArray(backing);
      this.bitsPerSegment = bitsPerSegment;
    }

    public int size() {
      return pool.length;
    }

    private int probeIndex() {
      if (bitSet.length() == 1) {
        return 0;
      }
      return ThreadLocalRandom.current().nextInt(bitSet.length());
    }

    @Override
    public abstract PooledObject<T> acquire();

    @Override
    public PooledObject<T> tryAcquire() {
      final int len = bitSet.length();
      int bitIndex = 0;
      int bitSetIndex = probeIndex() - 1;
      segments: for (int i = 0; i < len; i++) {
        bitSetIndex = (bitSetIndex + 1) % len;
        int state;
        do {
          state = bitSet.get(bitSetIndex);
          if ((bitIndex = Integer.lowestOneBit(state)) == 0) {
            // no objects found in this segment
            // break from CAS
            continue segments;
          }
        } while (!bitSet.compareAndSet(bitSetIndex, state, state & (~bitIndex)));
        // CAS success
        return pool[bitSetIndex * bitsPerSegment + Integer.numberOfTrailingZeros(bitIndex)];
      }
      // no objects found in any segment
      return null;
    }

    protected void release(final IndexedPooledObject pooledObject) {
      final int bitSetIndex = pooledObject.index / bitsPerSegment;
      final int bitIndex = pooledObject.index % bitsPerSegment;
      int state;
      do {
        state = bitSet.get(bitSetIndex);
      } while (!bitSet.compareAndSet(bitSetIndex, state, state | (1 << bitIndex)));
    }

    protected class IndexedPooledObject implements PooledObject<T> {
      protected final int index;
      private final T object;

      public IndexedPooledObject(final int index, final T object) {
        this.index = index;
        this.object = object;
      }

      @Override
      public void close() {
        release(this);
      }

      @Override
      public T get() {
        return object;
      }
    }
  }

  private static class SimplePooledObject<T> implements PooledObject<T> {
    private final T object;

    public SimplePooledObject(final T object) {
      this.object = object;
    }

    @Override
    public void close() {} // noop

    @Override
    public T get() {
      return object;
    }
  }
}
