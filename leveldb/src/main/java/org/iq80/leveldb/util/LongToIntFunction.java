package org.iq80.leveldb.util;

/**
 * @see <a
 * href="https://docs.oracle.com/javase/8/docs/api/java/util/function/LongToIntFunction.html">LongToIntFunction</a>
 */
public interface LongToIntFunction
{
    public int applyAsInt(long operand);
}
