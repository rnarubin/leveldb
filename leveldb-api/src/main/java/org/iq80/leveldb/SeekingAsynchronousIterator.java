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

package org.iq80.leveldb;

import java.util.Map.Entry;
import java.util.concurrent.CompletionStage;

public interface SeekingAsynchronousIterator<K, V>
    extends ReverseAsynchronousIterator<Entry<K, V>> {

  /**
   * positions the iterator such that the next element's key will be greater than or equal to the
   * given key (based on the given implementation's comparator)
   */
  public CompletionStage<Void> seek(K key);

  /**
   * positions the iterator at its beginning, where there are no preceding elements
   */
  public CompletionStage<Void> seekToFirst();

  /**
   * positions the iterator at its end, where there are no successive elements
   */
  public CompletionStage<Void> seekToEnd();
}


