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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.iq80.leveldb.util.ObjectPool.PooledObject;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ObjectPoolTest {
  @Test
  public void fixedAllocatingTest() throws InterruptedException, ExecutionException {
    final int initSize = 234567;
    final ObjectPool<DummyClass> pool;

    {
      final List<DummyClass> initObjects = new ArrayList<>(initSize);
      final AtomicInteger i = new AtomicInteger(0);
      for (int j = 0; j + 1 < initSize;) {
        initObjects.add(new DummyClass(j = i.getAndIncrement()));
      }
      pool = ObjectPools.fixedAllocatingPool(initObjects, () -> new DummyClass(i.getAndIncrement()),
          4);
    }

    {
      final List<PooledObject<DummyClass>> initAcqs = new ArrayList<>(initSize);
      for (int i = 0; i < initSize; i++) {
        final PooledObject<DummyClass> acq = i % 2 == 0 ? pool.acquire() : pool.tryAcquire();
        Assert.assertNotNull(acq);
        Assert.assertTrue(acq.get().id < initSize,
            "acquire did not return object from initial supply");
        initAcqs.add(acq);
      }

      for (int i = initSize; i < initSize + 10; i++) {
        final PooledObject<DummyClass> acq = pool.acquire();
        Assert.assertEquals(acq.get().id, i, "new acquries did not use supplied supplier");
        acq.close();
      }

      Assert.assertNull(pool.tryAcquire(), "tryAcquire when exhausted did not return null");

      for (int i = 0; i < 5; i++) {
        initAcqs.remove(initAcqs.size() - 1).close();
      }

      for (int i = 0; i < 5; i++) {
        final PooledObject<DummyClass> acq = pool.acquire();
        Assert.assertTrue(acq.get().id < initSize,
            "acquire did not return object from initial supply after releases");
        initAcqs.add(acq);
      }

      for (final PooledObject<?> o : initAcqs) {
        o.close();
      }
    }

    {
      final int threadCount = 8;
      final List<Callable<List<PooledObject<DummyClass>>>> work = new ArrayList<>(threadCount);
      for (int i = 0; i < threadCount; i++) {
        final int j = i;
        final int acqNum = (initSize / threadCount) + (i == 0 ? initSize % threadCount : 0);
        work.add(new Callable<List<PooledObject<DummyClass>>>() {
          @Override
          public List<PooledObject<DummyClass>> call() throws Exception {
            final List<PooledObject<DummyClass>> acqs = new ArrayList<>(acqNum);
            for (int i = 0; i < acqNum; i++) {
              PooledObject<DummyClass> acq = j % 2 == 0 ? pool.acquire() : pool.tryAcquire();
              if (i % 3 == 0) {
                acq.close();
                acq = j % 2 == 0 ? pool.acquire() : pool.tryAcquire();
              }
              acqs.add(acq);
            }
            return acqs;
          }
        });
      }
      try (ConcurrencyHelper<List<PooledObject<DummyClass>>> conc =
          new ConcurrencyHelper<>(threadCount, "fixedAllocatingTest")) {
        final List<List<PooledObject<DummyClass>>> results = conc.submitAllAndWait(work);

        Assert.assertNull(pool.tryAcquire());
        Assert.assertTrue(pool.acquire().get().id > initSize);

        for (final List<PooledObject<DummyClass>> workResult : results) {
          for (final PooledObject<DummyClass> o : workResult) {
            Assert.assertNotNull(o, "tryAcquire failed before object exhaustion");
            Assert.assertTrue(o.get().id < initSize,
                "acquire did not return object from initial supply");
            o.close();
          }
        }
      }
    }
  }

  private static class DummyClass {
    final int id;

    public DummyClass(final int id) {
      this.id = id;
    }
  }
}
