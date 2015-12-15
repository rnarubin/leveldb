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
package org.iq80.leveldb.impl;

import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.iq80.leveldb.Snapshot;

public class Snapshots
{
    // can't be a ConcurrentLinkedQueue due to possible races between sequence
    // get and insertion
    private final SortedSet<Long> set = new ConcurrentSkipListSet<>();

    public SnapshotImpl getSnapshot(long sequence)
    {
        SnapshotImpl s = new SnapshotImpl(sequence);
        set.add(sequence);
        return s;
    }

    public long getOldestSequence()
    {
        if (set.isEmpty()) {
            return -1;
        }
        return set.first();
    }

    class SnapshotImpl
            implements Snapshot
    {
        private final AtomicBoolean closed = new AtomicBoolean();
        private final long lastSequence;

        private SnapshotImpl(long lastSequence)
        {
            this.lastSequence = lastSequence;
        }

        @Override
        public void close()
        {
            // This is an end user API.. he might screw up and close multiple
            // times.
            if (closed.compareAndSet(false, true)) {
                set.remove(lastSequence);
            }
        }

        public long getLastSequence()
        {
            return lastSequence;
        }

        @Override
        public String toString()
        {
            return "SnapshotImpl [" + lastSequence + "]";
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + (int) (lastSequence ^ (lastSequence >>> 32));
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            SnapshotImpl other = (SnapshotImpl) obj;
            return lastSequence == other.lastSequence;
        }
    }
}
