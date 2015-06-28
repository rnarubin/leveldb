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

import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.ReverseSeekingIterator;
import org.iq80.leveldb.util.AbstractReverseSeekingIterator;
import org.iq80.leveldb.util.InternalIterator;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map.Entry;

import static org.iq80.leveldb.util.TwoStageIterator.CurrentOrigin.*;

public abstract class TwoStageIterator<I extends ReverseSeekingIterator<InternalKey, V> & Closeable,
                                       D extends ReverseSeekingIterator<InternalKey, ByteBuffer> & Closeable,
                                       V>
        extends AbstractReverseSeekingIterator<InternalKey, ByteBuffer>
        implements InternalIterator
{
    private final I index;
    private Closeable last;
    private D current;
    private CurrentOrigin currentOrigin = NONE;
    private boolean closed;

    static enum CurrentOrigin
    {
        /*
         * reversable iterators don't have a consistent concept of a "current" item instead they exist in a position "between" next and prev. in order to make the BlockIterator 'current' work, we need
         * to track from which direction it was initialized so that calls to advance the encompassing 'blockIterator' are consistent
         */
        PREV, NEXT, NONE
        // a state of NONE should be interchangeable with current==NULL
    }

    public TwoStageIterator(I indexIterator)
    {
        this.index = indexIterator;
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed) {
            closed = true;
            Closeables.closeIO(last, current, index);
        }
        else {
            // TODO check if this ever happens
            throw new IllegalStateException("double close on iterator");
        }
    }

    @Override
    protected final void seekToFirstInternal()
            throws IOException
    {
        // reset index to before first and clear the data iterator
        index.seekToFirst();
        Closeables.closeIO(last, current);
        last = null;
        current = null;
        currentOrigin = NONE;
    }

    @Override
    public final void seekToEndInternal()
            throws IOException
    {
        index.seekToEnd();
        Closeables.closeIO(last, current);
        last = null;
        current = null;
        currentOrigin = NONE;
    }

    @Override
    protected final void seekInternal(InternalKey targetKey)
            throws IOException
    {
        // seek the index to the block containing the key
        index.seek(targetKey);

        Closeables.closeIO(last, current);
        last = null;
        // if indexIterator does not have a next, it mean the key does not exist in this iterator
        if (index.hasNext()) {
            // seek the current iterator to the key
            current = getData(index.next().getValue());
            currentOrigin = NEXT;
            current.seek(targetKey);
        }
        else {
            current = null;
            currentOrigin = NONE;
        }
    }

    @Override
    protected final boolean hasNextInternal()
            throws IOException
    {
        return currentHasNext();
    }

    @Override
    protected final boolean hasPrevInternal()
            throws IOException
    {
        return currentHasPrev();
    }

    @Override
    protected final Entry<InternalKey, ByteBuffer> getNextElement()
            throws IOException
    {
        if (current != null) {
            Closeables.closeIO(last);
            last = null;
        }
        // note: it must be here & not where 'current' is assigned,
        // because otherwise we'll have called inputs.next() before throwing
        // the first NPE, and the next time around we'll call inputs.next()
        // again, incorrectly moving beyond the error.
        return currentHasNext() ? current.next() : null;
    }

    @Override
    protected final Entry<InternalKey, ByteBuffer> getPrevElement()
            throws IOException
    {
        if (current != null) {
            Closeables.closeIO(last);
            last = null;
        }
        return currentHasPrev() ? current.prev() : null;
    }

    @Override
    protected final Entry<InternalKey, ByteBuffer> peekInternal()
            throws IOException
    {
        return currentHasNext() ? current.peek() : null;
    }

    @Override
    protected final Entry<InternalKey, ByteBuffer> peekPrevInternal()
            throws IOException
    {
        return currentHasPrev() ? current.peekPrev() : null;
    }

    private boolean currentHasNext()
            throws IOException
    {
        boolean currentHasNext = false;
        while (true) {
            if (current != null) {
                currentHasNext = current.hasNext();
            }
            if (!currentHasNext) {
                if (currentOrigin == PREV) {
                    // current came from PREV, so advancing indexIterator to next() must be safe
                    // indeed, because alternating calls to prev() and next() must return the same item
                    // current can be retrieved from next() when the origin is PREV
                    index.next();
                    // but of course we want to go beyond current to the next block
                    // so we pass into the next if
                }
                if (index.hasNext()) {
                    Closeables.closeIO(last);
                    last = current;
                    current = getData(index.next().getValue());
                    currentOrigin = NEXT;
                    current.seekToFirst();
                }
                else {
                    if (current != null) {
                        Closeables.closeIO(last);
                        last = current;
                        current = null;
                        currentOrigin = NONE;
                    }
                    return false;
                }
            }
            else {
                return true;
            }
        }
    }

    private boolean currentHasPrev()
            throws IOException
    {
        boolean currentHasPrev = false;
        while (true) {
            if (current != null) {
                currentHasPrev = current.hasPrev();
            }
            if (!(currentHasPrev)) {
                if (currentOrigin == NEXT) {
                    index.prev();
                }
                if (index.hasPrev()) {
                    Closeables.closeIO(last);
                    last = current;
                    current = getData(index.prev().getValue());
                    currentOrigin = PREV;
                    current.seekToEnd();
                }
                else {
                    if (current != null) {
                        Closeables.closeIO(last);
                        last = current;
                        current = null;
                        currentOrigin = NONE;
                    }
                    return false;
                }
            }
            else {
                return true;
            }
        }
    }
    
    protected abstract D getData(V indexValue);

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("TwoStageIterator");
        sb.append("{index=").append(index);
        sb.append(", current=").append(current);
        sb.append('}');
        return sb.toString();
    }
}