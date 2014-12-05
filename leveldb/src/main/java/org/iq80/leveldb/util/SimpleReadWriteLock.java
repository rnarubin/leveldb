package org.iq80.leveldb.util;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * A simple read-write lock.
 * <br>
 * <br>simple in that it:
 * <li>is not reentrant</li>
 * <li>does not provide a means of downgrading writers, nor upgrading readers</li>
 * <li>does not support conditions</li>
 * <br>
 * it does, however, support a fairness policy
 */
public class SimpleReadWriteLock implements ReadWriteLock
{
   private final Semaphore semaphore;
   private final int maxPermits;
   private final Lock readLock, writeLock;
   
   public SimpleReadWriteLock()
   {
      this(false);
   }
   
   public SimpleReadWriteLock(boolean fair)
   {
      this(fair, Integer.MAX_VALUE);
   }

   public SimpleReadWriteLock(boolean fair, int maxReaders)
   {
      this.maxPermits = maxReaders;
      this.semaphore = new Semaphore(maxPermits, fair);
      this.readLock = new AcquireLock(1);
      this.writeLock = new AcquireLock(maxPermits);
   }

   @Override
   public Lock readLock()
   {
      return readLock;
   }

   @Override
   public Lock writeLock()
   {
      return writeLock;
   }
   
   private class AcquireLock implements Lock
   {
      private final int permits;
      public AcquireLock(int permits)
      {
         this.permits = permits;
      }
      @Override
      public void lock()
      {
         SimpleReadWriteLock.this.semaphore.acquireUninterruptibly(permits);
      }

      @Override
      public void lockInterruptibly() throws InterruptedException
      {
         SimpleReadWriteLock.this.semaphore.acquire(permits);
      }

      @Override
      public boolean tryLock()
      {
         return SimpleReadWriteLock.this.semaphore.tryAcquire(permits);
      }

      @Override
      public boolean tryLock(long time, TimeUnit unit) throws InterruptedException
      {
         return SimpleReadWriteLock.this.semaphore.tryAcquire(permits, time, unit);
      }

      @Override
      public void unlock()
      {
         SimpleReadWriteLock.this.semaphore.release(permits);
      }

      @Override
      public Condition newCondition()
      {
         throw new UnsupportedOperationException();
      }
   }
}
