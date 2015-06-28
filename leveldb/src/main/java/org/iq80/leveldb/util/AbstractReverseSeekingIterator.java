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

import org.iq80.leveldb.DBException;
import org.iq80.leveldb.impl.ReverseSeekingIterator;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

public abstract class AbstractReverseSeekingIterator<K, V>
        implements ReverseSeekingIterator<K, V>
{
    private Entry<K, V> rPeekedElement;
    private Entry<K, V> peekedElement;

    @Override
    public final void seekToFirst()
    {
        rPeekedElement = peekedElement = null;
        try {
            seekToFirstInternal();
        }
        catch (IOException e) {
            throw new DBException(e);
        }
    }

    @Override
    public final void seek(K targetKey)
    {
        rPeekedElement = peekedElement = null;
        try {
            seekInternal(targetKey);
        }
        catch (IOException e) {
            throw new DBException(e);
        }
    }

    @Override
    public final void seekToEnd()
    {
        rPeekedElement = peekedElement = null;
        try {
            seekToEndInternal();
        }
        catch (IOException e) {
            throw new DBException(e);
        }
    }

    @Override
    public final boolean hasNext()
    {
        try {
            return peekedElement != null || hasNextInternal();
        }
        catch (IOException e) {
            throw new DBException(e);
        }
    }

    @Override
    public final boolean hasPrev()
    {
        try {
            return rPeekedElement != null || hasPrevInternal();
        }
        catch (IOException e) {
            throw new DBException(e);
        }
    }

    @Override
    public final Entry<K, V> next()
    {
        peekedElement = null;
        Entry<K, V> next;
        try {
            next = getNextElement();
        }
        catch (IOException e) {
            throw new DBException(e);
        }
        if (next == null) {
            throw new NoSuchElementException();
        }
        rPeekedElement = next;
        return next;
    }

    @Override
    public final Entry<K, V> prev()
    {
        rPeekedElement = null;
        Entry<K, V> prev;
        try {
            prev = getPrevElement();
        }
        catch (IOException e) {
            throw new DBException(e);
        }
        if (prev == null) {
            throw new NoSuchElementException();
        }
        peekedElement = prev;
        return prev;
    }

    @Override
    public final Entry<K, V> peek()
    {
        if (peekedElement == null) {
            try {
                peekedElement = peekInternal();
            }
            catch (IOException e) {
                throw new DBException(e);
            }
            if (peekedElement == null) {
                throw new NoSuchElementException();
            }
        }
        return peekedElement;
    }

    @Override
    public final Entry<K, V> peekPrev()
    {
        if (rPeekedElement == null) {
            try {
                rPeekedElement = peekPrevInternal();
            }
            catch (IOException e) {
                throw new DBException(e);
            }
            if (rPeekedElement == null) {
                throw new NoSuchElementException();
            }
        }
        return rPeekedElement;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    protected abstract Entry<K, V> peekInternal()
            throws IOException;

    protected abstract Entry<K, V> peekPrevInternal()
            throws IOException;

    protected abstract void seekToFirstInternal()
            throws IOException;

    protected abstract void seekToEndInternal()
            throws IOException;

    protected abstract void seekInternal(K targetKey)
            throws IOException;

    protected abstract boolean hasNextInternal()
            throws IOException;

    protected abstract boolean hasPrevInternal()
            throws IOException;

    protected abstract Entry<K, V> getNextElement()
            throws IOException;

    protected abstract Entry<K, V> getPrevElement()
            throws IOException;
}
