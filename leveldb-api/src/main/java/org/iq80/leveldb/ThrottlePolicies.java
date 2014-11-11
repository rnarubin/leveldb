/**
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.iq80.leveldb;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class ThrottlePolicies
{
    /**
     * This policy enforces a limit on pending writes. All writes
     * after this limit will block until pending writes are written
     * @param limit - number of pending writes after which new writes will block
     */
    public static ThrottlePolicy blockingLimitThrottle(int limit)
    {
       return new LimitThrottle(limit);
    }

    private static class LimitThrottle implements ThrottlePolicy
    {
        private final Semaphore sem;

        public LimitThrottle(int limit)
        {
           this.sem = new Semaphore(limit);
        }

        @Override
        public void throttleIfNecessary(int pendingWrites)
        {
            try {
               sem.acquire();
            }
            catch(InterruptedException propagate){
               Thread.currentThread().interrupt();
            }
        }

       @Override
       public void notifyCompletion()
       {
          sem.release();
       }
    }

    /**
     * This policy gradually slows writing attempts as pending writes accumulate,
     * avoiding the high variability introduced by blocking policies
     * @param tolerance - number of pending writes before throttling begins
     * @param progression - additional time (in ms) by which threads will be slowed for each pending write beyond tolerance
     */
    public static ThrottlePolicy progressiveThrottle(int tolerance, int progression)
    {
       return new ProgressiveThrottle(tolerance, progression);
    }

    private static class ProgressiveThrottle implements ThrottlePolicy
    {
       private final int tolerance, progression;
       
       public ProgressiveThrottle(int tolerance, int progression)
       {
           this.tolerance = tolerance;
           this.progression = progression;
       }

       @Override
       public void throttleIfNecessary(int pendingWrites)
       {
           int diff = pendingWrites - tolerance;
           if(diff > 0)
           {
              try
              {
                 Thread.sleep(diff*progression);
              }
              catch (InterruptedException propagate)
              {
                 Thread.currentThread().interrupt();
              }
           }
       }

       @Override
       public void notifyCompletion(){} // no op
    };
    
    /**
     * This policy performs no throttling. Consequently, the queue of pending writes
     * may grow without bound, and any writes not flushed to disk may be lost in the
     * event of an application crash 
     */
    public static ThrottlePolicy noThrottle() {
       return new ThrottlePolicy() {
         @Override
         public void throttleIfNecessary(int pendingWrites){}
         @Override
         public void notifyCompletion(){}
      };
    }
}