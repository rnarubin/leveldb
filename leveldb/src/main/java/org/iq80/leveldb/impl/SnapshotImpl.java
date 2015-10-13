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
package org.iq80.leveldb.impl;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongConsumer;

import org.iq80.leveldb.Snapshot;

public class SnapshotImpl implements Snapshot {
  private final AtomicBoolean closed = new AtomicBoolean();
  private final long lastSequence;
  private final LongConsumer cleanup;

  public SnapshotImpl(final long lastSequence, final LongConsumer cleanup) {
    this.lastSequence = lastSequence;
    this.cleanup = cleanup;
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      this.cleanup.accept(lastSequence);
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
