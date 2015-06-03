package org.iq80.leveldb.util;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;

import sun.nio.ch.DirectBuffer;
import sun.nio.ch.FileChannelImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("restriction")
public final class ByteBuffers
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ByteBuffers.class);

    private ByteBuffers()
    {
    }

    private interface BufferUtil
    {
        // ByteBuffer duplicate(ByteBuffer src, int position, int limit);
    }

    private static final BufferUtil UTIL = PureJavaUtil.INSTANCE;

    private enum PureJavaUtil
            implements BufferUtil
    {
        INSTANCE;
    }

    public static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0).order(ByteOrder.LITTLE_ENDIAN);

    private static final boolean directBufferSupport;
    static {
        boolean success = false;
        try {
            ((DirectBuffer) ByteBuffer.allocateDirect(8)).cleaner().clean();
            success = true;
        }
        catch (Throwable t) {
            LOGGER.debug("failed to access DirectBuffer support", t);
            success = false;
        }
        finally {
            LOGGER.debug("DirectBuffer support:", success);
            directBufferSupport = success;
        }
    }

    public static void freeDirect(ByteBuffer buffer)
    {
        if (directBufferSupport && buffer.isDirect()) {
            ((DirectBuffer) buffer).cleaner().clean();
        }
        // else
        // leave it to Java GC
    }

    private static final Method unmap;
    static {
        Method x;
        try {
            x = FileChannelImpl.class.getDeclaredMethod("unmap", MappedByteBuffer.class);
            x.setAccessible(true);
        }
        catch (Throwable t) {
            LOGGER.debug("failed to access MappedByteBuffer support", t);
            x = null;
        }
        unmap = x;
    }

    public static void unmap(MappedByteBuffer buffer)
            throws IOException
    {
        if (unmap != null) {
            try {
                unmap.invoke(null, buffer);
            }
            catch (Exception e) {
                throw new IOException("Failed to unmap MappedByteBuffer", e);
            }
        }
        // else
        // leave it to Java GC
    }

    public static void writeLengthPrefixedBytes(ByteBuffer dst, ByteBuffer src)
    {
        VariableLengthQuantity.writeVariableLengthInt(src.remaining(), dst);
        dst.put(src);
    }

    public static ByteBuffer duplicate(ByteBuffer src, int position, int limit)
    {
        ByteBuffer ret = duplicate(src);
        ret.limit(limit).position(position);
        return ret;
    }

    /**
     * buffer duplication which preserves byte order
     */
    public static ByteBuffer duplicate(ByteBuffer src)
    {
        return src.duplicate().order(src.order());
    }

    public static short readUnsignedByte(ByteBuffer src)
    {
        return (short) (src.get() & 0xFF);
    }

    public static ByteBuffer sliceAndAdvance(ByteBuffer src, int length)
    {
        final int oldpos = src.position();
        src.position(oldpos + length);
        return duplicate(src, oldpos, oldpos + length);
    }
}
