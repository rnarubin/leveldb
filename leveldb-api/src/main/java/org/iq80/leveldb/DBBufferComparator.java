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

package org.iq80.leveldb;

import java.nio.ByteBuffer;
import java.util.Comparator;

public interface DBBufferComparator
        extends Comparator<ByteBuffer>
{
    /**
     * The name of the comparator. Used to check for comparator mismatches
     * (i.e., a DB created with one comparator is accessed using a different
     * comparator)
     */
    String name();

    /**
     * If <code>start < limit</code>, mutate {@code start} to be a short key in
     * [start,limit). Simple comparator implementations may leave {@code start}
     * unchanged.
     * 
     * @return true iff start was mutated
     */
    boolean findShortestSeparator(ByteBuffer start, ByteBuffer limit);

    /**
     * mutate {@code key} to be a 'short key' where the 'short key' >= key.
     * Simple comparator implementations may leave {@code key} unchanged.
     * 
     * @return true iff key was mutated
     */
    boolean findShortSuccessor(ByteBuffer key);
}
