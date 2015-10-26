/*
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

package com.cleversafe.leveldb.util;

public interface ObjectPool<T>
{
    /**
     * Exclusively acquire an object from the set of available objects in the pool, removing it from
     * the available set. The returned {@link PooledObject} must be {@link PooledObject#close()
     * closed} in order to become available again. Behavior when the pool has no available
     * objects is defined by implementing classes
     */
    public PooledObject<T> acquire();

    /**
     * Try to acquire an object from the set of available objects in the pool, removing it from the
     * available set. If no objects are available, {@code null} is returned.
     */
    public PooledObject<T> tryAcquire();

    /**
     * A closeable wrapper around an object belonging to an object pool. A PooledObject is implicitly
     * opened upon calling {@link ObjectPool#acquire()} and must be {@link #close() closed} in
     * order to become available again
     */
    public interface PooledObject<T>
            extends AutoCloseable
    {
        public T get();

        @Override
        public void close();
    }
}
