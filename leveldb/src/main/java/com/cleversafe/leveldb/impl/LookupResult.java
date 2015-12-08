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

import java.nio.ByteBuffer;

public class LookupResult {
  public static LookupResult ok(final LookupKey key, final ByteBuffer value) {
    return new LookupResult(key, value);
  }

  public static LookupResult deleted(final LookupKey key) {
    return new LookupResult(key, null);
  }

  private final LookupKey key;
  private final ByteBuffer value;

  private LookupResult(final LookupKey key, final ByteBuffer value) {
    assert key != null;
    this.key = key;
    this.value = value;
  }

  public LookupKey getKey() {
    return key;
  }

  public ByteBuffer getValue() {
    return value;
  }

  public boolean isDeleted() {
    return getValue() == null;
  }

  @Override
  public String toString() {
    return "LookupResult [key=" + key + ", value=" + value + "]";
  }
}
