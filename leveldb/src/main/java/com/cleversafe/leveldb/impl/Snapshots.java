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
package com.cleversafe.leveldb.impl;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.cleversafe.leveldb.Snapshot;

public class Snapshots {
  private final Queue<Long> openSnapshots = new ConcurrentLinkedQueue<>();

  public SnapshotImpl newSnapshot(final long lastSequence) {
    openSnapshots.add(lastSequence);
    return new SnapshotImpl(lastSequence);
  }

  public class SnapshotImpl implements Snapshot {
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final long lastSequence;

    public SnapshotImpl(final long lastSequence) {
      this.lastSequence = lastSequence;
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        openSnapshots.remove(lastSequence);
      }
    }

    public long getLastSequence() {
      return lastSequence;
    }

    @Override
    public String toString() {
      return "SnapshotImpl [lastSequence=" + lastSequence + ", closed=" + closed + "]";
    }
  }

}
