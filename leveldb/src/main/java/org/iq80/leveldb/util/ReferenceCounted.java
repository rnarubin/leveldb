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

package org.iq80.leveldb.util;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class ReferenceCounted<T>
        implements AutoCloseable
{
    AtomicInteger refCount = new AtomicInteger(1);

    public final T retain()
    {
        int count;
        do {
            count = refCount.get();
            if (count == 0) {
                // raced with a final release,
                // force the caller to reacquire based on context
                return null;
            }
        }
        while (!refCount.compareAndSet(count, count + 1));
        return getThis();
    }

    public final void release()
    {
        final int count = refCount.decrementAndGet();
        if (count == 0) {
            dispose();
        }
        else if (count < 0) {
            throw new IllegalStateException("release called more than retain");
        }
    }

    protected abstract T getThis();

    protected abstract void dispose();

    protected final AtomicInteger getReferenceCount()
    {
        return refCount;
    }

    @Override
    public final void close()
    {
        release();
    }
}
