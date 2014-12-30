package org.iq80.leveldb.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.iq80.leveldb.util.ObjectPool.PooledObject;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Supplier;

public class ObjectPoolTest
{
   @Test
   public void fixedAllocatingTest() throws InterruptedException, ExecutionException
   {
      final int initSize = 234567;
      final ObjectPool<DummyClass> pool;

      {
         List<DummyClass> initObjects = new ArrayList<>(initSize);
         final int i[] = new int[1];
         for(i[0] = 0; i[0] < initSize;){
            initObjects.add(new DummyClass(i[0]++));
         }
         pool = ObjectPools.fixedAllocatingPool(initObjects, new Supplier<DummyClass>(){
            @Override
            public DummyClass get()
            {
               return new DummyClass(i[0]++);
            }
         });
      }
      
      {
         List<PooledObject<DummyClass>> initAcqs = new ArrayList<>(initSize);
         for(int i = 0; i < initSize; i++) {
            PooledObject<DummyClass> acq = i%2==0 ? pool.acquire() : pool.tryAcquire();
            Assert.assertNotNull(acq);
            Assert.assertTrue(acq.get().id < initSize, "acquire did not return object from initial supply");
            initAcqs.add(acq);
         }
         
         for(int i = initSize; i < initSize+10; i++) {
            PooledObject<DummyClass> acq = pool.acquire();
            Assert.assertEquals(acq.get().id, i, "new acquries did not use supplied supplier");
            acq.close();
         }
         
         Assert.assertNull(pool.tryAcquire(), "tryAcquire when exhausted did not return null");
         
         for(int i = 0; i < 5; i++) {
            initAcqs.remove(initAcqs.size()-1).close();
         }

         for(int i = 0; i < 5; i++) {
            PooledObject<DummyClass> acq = pool.acquire();
            Assert.assertTrue(acq.get().id < initSize, "acquire did not return object from initial supply after releases");
            initAcqs.add(acq);
         }
         
         for(PooledObject<?> o : initAcqs) {
            o.close();
         }
      }
      
      {
         final int threadCount = 8;
         List<Callable<List<PooledObject<DummyClass>>>> work = new ArrayList<>(threadCount);
         for(int i = 0; i < threadCount; i++){
            final int j = i;
            final int acqNum = (initSize/threadCount) + (i==0?initSize%threadCount : 0);
            work.add(new Callable<List<PooledObject<DummyClass>>>(){
               public List<PooledObject<DummyClass>> call() throws Exception
               {
                  List<PooledObject<DummyClass>> acqs = new ArrayList<>(acqNum);
                  for(int i = 0; i < acqNum; i++){
                     PooledObject<DummyClass> acq = j%2==0 ? pool.acquire() : pool.tryAcquire();
                     if(i%3 == 0){
                        acq.close();
                        acq = j%2==0 ? pool.acquire() : pool.tryAcquire();
                     }
                     acqs.add(acq);
                  }
                  return acqs;
               }
            });
         }
         try(ConcurrencyHelper<List<PooledObject<DummyClass>>> conc = new ConcurrencyHelper<>(threadCount)) {
            conc.submitAll(work);
            List<List<PooledObject<DummyClass>>> results = conc.waitForFinish();

            Assert.assertNull(pool.tryAcquire());
            Assert.assertTrue(pool.acquire().get().id > initSize);

            for(List<PooledObject<DummyClass>> workResult: results) {
               for(PooledObject<DummyClass> o : workResult) {
                  Assert.assertNotNull(o, "tryAcquire failed before object exhaustion");
                  Assert.assertTrue(o.get().id < initSize, "acquire did not return object from initial supply");
                  o.close();
               }
            }
         }
      }
   }
   
   private static class DummyClass
   {
      final int id;
      public DummyClass(final int id)
      {
         this.id = id;
      }
   }
}
