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

package com.cleversafe.leveldb.util;

import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.IntFunction;

public class DeletionQueue<T> implements Iterable<T> {
  private final Deque<T> deque = new ConcurrentLinkedDeque<>();

  public DeletionHandle<T> insertFirst(final T t) {
    deque.addFirst(t);
    return getHandle(deque.iterator(), t);
  }

  public DeletionHandle<T> insert(final T t) {
    // TODO(maybe) reduce contention by random first/last
    return insertFirst(t);
  }

  private static <T> DeletionHandle<T> getHandle(final Iterator<T> iter, final T target) {
    T next;
    do {
      assert iter.hasNext();
      next = iter.next();
    } while (next != target);
    return new DeletionHandle<T>(target) {
      @Override
      public void close() {
        iter.remove();
      }
    };
  }

  public T peekLast() {
    return deque.peekLast();
  }

  @Override
  public Iterator<T> iterator() {
    return deque.iterator();
  }

  public T[] toArray(final IntFunction<T[]> arrSupplier) {
    return deque.stream().toArray(arrSupplier);
  }

  public static abstract class DeletionHandle<T> implements AutoCloseable {
    public final T item;

    public DeletionHandle(final T t) {
      this.item = t;
    }

    @Override
    public abstract void close();
  }
}
