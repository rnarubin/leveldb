package org.iq80.leveldb.util;

import org.iq80.leveldb.impl.ReverseSeekingIterator;

import java.util.Map.Entry;
import java.util.NoSuchElementException;

public abstract class AbstractReverseSeekingIterator<K, V> extends AbstractSeekingIterator<K, V> implements ReverseSeekingIterator<K, V>
{
    protected Entry<K, V> prevElement;

    @Override
    public final void seekToFirst()
    {
        prevElement = null;
        super.seekToFirst();
    }
    
    @Override
    public void seekToLast(){
        prevElement = null;
        nextElement = null;
        seekToLastInternal();
    }

    @Override
    public final void seek(K targetKey)
    {
        prevElement = null;
        nextElement = null;
        seekInternal(targetKey);
    }

    @Override
    public final boolean hasPrev(){
       if(prevElement == null){
          prevElement = getPrevElement();
       }
       return prevElement != null;
    }

    @Override
    public final Entry<K, V> next()
    {
       prevElement = nextElement;
       return super.next();
    }
    
    @Override
    public final Entry<K, V> prev(){
       nextElement = prevElement;
        if (prevElement == null) {
            prevElement = getPrevElement();
            if (prevElement == null) {
                throw new NoSuchElementException();
            }
        }

        Entry<K, V> result = prevElement;
        prevElement = null;
        return result;
    }

    @Override
    public final Entry<K, V> peekPrev()
    {
        if (prevElement == null) {
            prevElement = getPrevElement();
            if (prevElement == null) {
                throw new NoSuchElementException();
            }
        }

        return prevElement;
    }

    protected abstract void seekToLastInternal();
    protected abstract Entry<K, V> getPrevElement();
}