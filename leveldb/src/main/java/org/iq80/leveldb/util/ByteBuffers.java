package org.iq80.leveldb.util;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;

import sun.nio.ch.DirectBuffer;
import sun.nio.ch.FileChannelImpl;
import sun.misc.Unsafe;

import org.iq80.leveldb.MemoryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.iq80.leveldb.util.PureJavaCrc32C.T;
import static org.iq80.leveldb.util.PureJavaCrc32C.T8_0_start;
import static org.iq80.leveldb.util.PureJavaCrc32C.T8_1_start;
import static org.iq80.leveldb.util.PureJavaCrc32C.T8_2_start;
import static org.iq80.leveldb.util.PureJavaCrc32C.T8_3_start;
import static org.iq80.leveldb.util.PureJavaCrc32C.T8_4_start;
import static org.iq80.leveldb.util.PureJavaCrc32C.T8_5_start;
import static org.iq80.leveldb.util.PureJavaCrc32C.T8_6_start;
import static org.iq80.leveldb.util.PureJavaCrc32C.T8_7_start;

@SuppressWarnings("restriction")
public final class ByteBuffers
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ByteBuffers.class);

    private ByteBuffers()
    {
    }

    interface BufferUtil
    {
        int calculateSharedBytes(ByteBuffer leftKey, ByteBuffer rightKey);

        void putZero(ByteBuffer dst, int length);

        int compare(ByteBuffer buffer1, int offset1, int length1, ByteBuffer buffer2, int offset2, int length2);

        ByteBufferCrc32 crc32();
    }

    private static final BufferUtil UTIL = PureJavaUtil.INSTANCE;// getBestUtil();

    /**
     * Returns the Unsafe-using util, or falls back to the pure-Java implementation if unable to do so.
     */
    private static BufferUtil getBestUtil()
    {
        final String UNSAFE_UTIL_NAME = ByteBuffers.class.getName() + "$UnsafeUtil";
        try {
            BufferUtil util = (BufferUtil) Class.forName(UNSAFE_UTIL_NAME).getEnumConstants()[0];
            LOGGER.debug("Successfully loaded unsafe util");
            return util;
        }
        catch (Throwable t) {
            LOGGER.debug("Failed to load unsafe util, falling back to java impl ({})", t);
            return PureJavaUtil.INSTANCE;
        }
    }

    private enum PureJavaUtil
            implements BufferUtil
    {
        INSTANCE;

        @Override
        public int calculateSharedBytes(ByteBuffer leftKey, ByteBuffer rightKey)
        {
            int sharedKeyBytes = 0;
            final int lpos = leftKey.position(), rpos = rightKey.position();

            if (leftKey != null && rightKey != null) {
                int minSharedKeyBytes = Math.min(leftKey.remaining(), rightKey.remaining());
                while (sharedKeyBytes < minSharedKeyBytes
                        && leftKey.get(lpos + sharedKeyBytes) == rightKey.get(rpos + sharedKeyBytes)) {
                    sharedKeyBytes++;
                }
            }

            return sharedKeyBytes;
        }

        @Override
        public void putZero(ByteBuffer dst, int length)
        {
            for (; length > 0; length--) {
                dst.put((byte) 0);
            }
        }

        @Override
        public int compare(ByteBuffer buffer1, int offset1, int length1, ByteBuffer buffer2, int offset2, int length2)
        {
            if (buffer1 == buffer2 && offset1 == offset2 && length1 == length2) {
                return 0;
            }
            final int len = Math.min(length1, length2);
            for (int i = 0; i < len; i++) {
                int a = (buffer1.get(offset1 + i) & 0xff);
                int b = (buffer2.get(offset2 + i) & 0xff);
                if (a != b) {
                    return a - b;
                }
            }
            return length1 - length2;
        }

        @Override
        public ByteBufferCrc32 crc32()
        {
            return new PureJavaCrc32C();
        }
    }

    @SuppressWarnings("unused")
    private enum UnsafeUtil
            implements BufferUtil
    {
        INSTANCE;

        private static final Unsafe unsafe;

        private static final long DIRECT_ADDRESS_FIELD_OFFSET;
        private static final long BYTE_ARRAY_FIELD_OFFSET;
        private static final long BYTE_ARRAY_OFFSET_FIELD_OFFSET;
        private static final boolean BIG_ENDIAN = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

        static {
            unsafe = (Unsafe) AccessController.doPrivileged(new PrivilegedAction<Object>()
            {
                @Override
                public Object run()
                {
                    try {
                        final Field f = Unsafe.class.getDeclaredField("theUnsafe");
                        f.setAccessible(true);
                        return f.get(null);
                    }
                    catch (final NoSuchFieldException e) {
                        // It doesn't matter what we throw;
                        // it's swallowed in getBestComparer().
                        throw new Error(e);
                    }
                    catch (final IllegalAccessException e) {
                        throw new Error(e);
                    }
                }
            });

            // sanity check - this should never fail
            if (unsafe.arrayIndexScale(byte[].class) != 1) {
                throw new AssertionError();
            }

            try {
                DIRECT_ADDRESS_FIELD_OFFSET = getFieldOffset(Buffer.class, "address");
                BYTE_ARRAY_FIELD_OFFSET = getFieldOffset(ByteBuffer.class, "hb");
                BYTE_ARRAY_OFFSET_FIELD_OFFSET = getFieldOffset(ByteBuffer.class, "offset");
            }
            catch (NoSuchFieldException | SecurityException e) {
                throw new Error(e);
            }
        }

        private static long getFieldOffset(final Class<?> clazz, final String fieldName)
                throws NoSuchFieldException, SecurityException
        {
            final Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            return unsafe.objectFieldOffset(field);
        }

        @Override
        public int calculateSharedBytes(ByteBuffer leftKey, ByteBuffer rightKey)
        {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public void putZero(ByteBuffer dst, int length)
        {
            // TODO Auto-generated method stub

        }

        @Override
        public int compare(ByteBuffer buffer1, int offset1, int length1, ByteBuffer buffer2, int offset2, int length2)
        {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public ByteBufferCrc32 crc32()
        {
            return new UnsafeCrc32c();
        }

        private static class UnsafeCrc32c
                implements ByteBufferCrc32
        {
            private int crc;

            public UnsafeCrc32c()
            {
                reset();
            }

            @Override
            public long getValue()
            {
                return (~crc) & 0xffffffffL;
            }

            public int getIntValue()
            {
                return ~crc;
            }

            @Override
            public void reset()
            {
                crc = 0xffffffff;
            }


            @Override
            final public void update(int b)
            {
                crc = (crc >>> 8) ^ T[T8_0_start + ((crc ^ b) & 0xff)];
            }

            @Override
            public void update(byte[] b, int off, int len)
            {
                crc = updateArray(crc, b, off + Unsafe.ARRAY_BYTE_BASE_OFFSET, len);
            }

            private static int updateArray(int localCrc, Object arr, long address, int len)
            {
                if(len > 7){
                    // first align to 8 bytes (or simply complete if len < 8)
                    final int init = (int) (address & 7);
                    switch (init) {
                        case 7: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(arr, address++)) & 0xff)];
                        case 6: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(arr, address++)) & 0xff)];
                        case 5: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(arr, address++)) & 0xff)];
                        case 4: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(arr, address++)) & 0xff)];
                        case 3: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(arr, address++)) & 0xff)];
                        case 2: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(arr, address++)) & 0xff)];
                        case 1: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(arr, address++)) & 0xff)];
                        default: // case 0, do nothing
                    }
                    len -= init;
                    
                    while (len > 7) {
                        long c = unsafe.getLong(arr, address);
                        if (BIG_ENDIAN) {
                            // this could probably be done natively with the appropriate shifting.
                            // but i'm lazy; my apologies to anyone in big endian land
                            c = Long.reverseBytes(c);
                        }
                        final int highlong = (int) (c >>> 32);
                        localCrc ^= (int) c;
                        localCrc = (T[T8_7_start +  (localCrc         & 0xff)] ^ T[T8_6_start + ((localCrc >>>  8) & 0xff)]) ^
                                   (T[T8_5_start + ((localCrc >>> 16) & 0xff)] ^ T[T8_4_start + ((localCrc >>> 24) & 0xff)]) ^
                                   (T[T8_3_start +  (highlong         & 0xff)] ^ T[T8_2_start + ((highlong >>>  8) & 0xff)]) ^
                                   (T[T8_1_start + ((highlong >>> 16) & 0xff)] ^ T[T8_0_start + ((highlong >>> 24) & 0xff)]);
                    
                        address += 8;
                        len -= 8;
                    }
                }

                switch (len) {
                    case 7: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(arr, address++)) & 0xff)];
                    case 6: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(arr, address++)) & 0xff)];
                    case 5: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(arr, address++)) & 0xff)];
                    case 4: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(arr, address++)) & 0xff)];
                    case 3: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(arr, address++)) & 0xff)];
                    case 2: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(arr, address++)) & 0xff)];
                    case 1: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(arr, address++)) & 0xff)];
                    default: // case 0, do nothing
                }
                return localCrc;
            }

            private static int updateDirect(int localCrc, long address, int len)
            {
                if(len > 7){
                    // first align to 8 bytes (or simply complete if len < 8)
                    final int init = (int) (address & 7);
                    switch (init) {
                        case 7: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(address++)) & 0xff)];
                        case 6: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(address++)) & 0xff)];
                        case 5: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(address++)) & 0xff)];
                        case 4: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(address++)) & 0xff)];
                        case 3: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(address++)) & 0xff)];
                        case 2: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(address++)) & 0xff)];
                        case 1: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(address++)) & 0xff)];
                        default: // case 0, do nothing
                    }
                    len -= init;
                    
                    while (len > 7) {
                        long c = unsafe.getLong(address);
                        if (BIG_ENDIAN) {
                            // this could probably be done natively with the appropriate shifting.
                            // but i'm lazy; my apologies to anyone in big endian land
                            c = Long.reverseBytes(c);
                        }
                        final int highlong = (int) (c >>> 32);
                        localCrc ^= (int) c;
                        localCrc = (T[T8_7_start +  (localCrc         & 0xff)] ^ T[T8_6_start + ((localCrc >>>  8) & 0xff)]) ^
                                   (T[T8_5_start + ((localCrc >>> 16) & 0xff)] ^ T[T8_4_start + ((localCrc >>> 24) & 0xff)]) ^
                                   (T[T8_3_start +  (highlong         & 0xff)] ^ T[T8_2_start + ((highlong >>>  8) & 0xff)]) ^
                                   (T[T8_1_start + ((highlong >>> 16) & 0xff)] ^ T[T8_0_start + ((highlong >>> 24) & 0xff)]);
                    
                        address += 8;
                        len -= 8;
                    }
                }

                switch (len) {
                    case 7: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(address++)) & 0xff)];
                    case 6: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(address++)) & 0xff)];
                    case 5: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(address++)) & 0xff)];
                    case 4: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(address++)) & 0xff)];
                    case 3: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(address++)) & 0xff)];
                    case 2: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(address++)) & 0xff)];
                    case 1: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(address++)) & 0xff)];
                    default: // case 0, do nothing
                }
                return localCrc;
            }


            @Override
            public void update(ByteBuffer b, int off, int len)
            {
                if(b.isDirect())
                {
                    crc = updateDirect(crc, off + unsafe.getLong(b, DIRECT_ADDRESS_FIELD_OFFSET), len);
                    
                }
                else{
                    crc = updateArray(crc, unsafe.getObject(b, BYTE_ARRAY_FIELD_OFFSET),
                            off + unsafe.getInt(b, BYTE_ARRAY_OFFSET_FIELD_OFFSET) + Unsafe.ARRAY_BYTE_BASE_OFFSET, len);
                }
            }
        }

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

    public static int getRemaining(ByteBuffer[] bufs)
    {
        int size = 0;
        for(ByteBuffer buf : bufs)
        {
            size+=buf.remaining();
        }
        return size;
    }

    public static void writeLengthPrefixedBytes(GrowingBuffer buffer, ByteBuffer src)
    {
        VariableLengthQuantity.writeVariableLengthInt(src.remaining(), buffer);
        buffer.put(src);
    }

    public static void writeLengthPrefixedBytesTransparent(GrowingBuffer buffer, ByteBuffer src)
    {
        src.mark();
        writeLengthPrefixedBytes(buffer, src);
        src.reset();
    }

    public static void writeLengthPrefixedBytesTransparent(ByteBuffer buffer, ByteBuffer src)
    {
        src.mark();
        writeLengthPrefixedBytes(buffer, src);
        src.reset();
    }

    public static void writeLengthPrefixedBytes(GrowingBuffer buffer, ByteBuffer[] srcs)
    {
        VariableLengthQuantity.writeVariableLengthInt(getRemaining(srcs), buffer);
        for (ByteBuffer src : srcs) {
            buffer.put(src);
        }
    }

    public static ByteBuffer readLengthPrefixedBytes(ByteBuffer src)
    {
        int length = VariableLengthQuantity.readVariableLengthInt(src);
        return duplicateAndAdvance(src, length);
    }

    /**
     * buffer duplication which preserves byte order
     */
    public static ByteBuffer duplicate(ByteBuffer src)
    {
        return src.duplicate().order(src.order());
    }

    public static ByteBuffer duplicate(ByteBuffer src, int position, int limit)
    {
        ByteBuffer ret = duplicate(src);
        ret.limit(limit).position(position);
        return ret;
    }

    public static ByteBuffer duplicateByLength(ByteBuffer src, int position, int length)
    {
        return duplicate(src, position, position + length);
    }

    public static ByteBuffer slice(ByteBuffer src)
    {
        return src.slice().order(src.order());
    }

    public static int readUnsignedByte(ByteBuffer src)
    {
        return src.get() & 0xFF;
    }

    public static int getUnsignedByte(ByteBuffer src, int position)
    {
        return src.get(position) & 0xFF;
    }

    public static ByteBuffer duplicateAndAdvance(ByteBuffer src, int length)
    {
        final int oldpos = src.position();
        src.position(oldpos + length);
        return duplicate(src, oldpos, oldpos + length);
    }

    public static int calculateSharedBytes(ByteBuffer leftKey, ByteBuffer rightKey)
    {
        return UTIL.calculateSharedBytes(leftKey, rightKey);
    }

    public static ByteBuffer putZero(ByteBuffer dst, int length)
    {
        UTIL.putZero(dst, length);
        return dst;
    }

    public static int compare(ByteBuffer a, ByteBuffer b)
    {
        return UTIL.compare(a, a.position(), a.remaining(), b, b.position(), b.remaining());
    }

    public static ByteBuffer copy(ByteBuffer src, int length, MemoryManager memory)
    {
        ByteBuffer ret = memory.allocate(length).put(duplicateByLength(src, src.position(), length));
        ret.rewind();
        return ret;
    }

    public static ByteBuffer copy(ByteBuffer src, MemoryManager memory)
    {
        return copy(src, src.remaining(), memory);
    }

    public static byte[] toArray(ByteBuffer src, byte[] dst)
    {
        src.mark();
        src.get(dst);
        src.reset();
        return dst;
    }

    public static byte[] toArray(ByteBuffer src)
    {
        return toArray(src, new byte[src.remaining()]);
    }

    public static String toString(ByteBuffer src)
    {
        return new String(toArray(src), StandardCharsets.UTF_8);
    }

    /**
     * @return dst
     */
    public static ByteBuffer putLength(ByteBuffer dst, ByteBuffer src, int length)
    {
        int oldlim = src.limit();
        src.limit(src.position() + length);
        dst.put(src);
        src.limit(oldlim);
        return dst;
    }

    public static ByteBufferCrc32 crc32()
    {
        return UTIL.crc32();
    }

    private static final int MASK_DELTA = 0xa282ead8;

    /**
     * Return a masked representation of crc.
     * <p/>
     * Motivation: it is problematic to compute the CRC of a string that
     * contains embedded CRCs.  Therefore we recommend that CRCs stored
     * somewhere (e.g., in files) should be masked before being stored.
     */
    public static int maskChecksum(int crc)
    {
        // Rotate right by 15 bits and add a constant.
        return ((crc >>> 15) | (crc << 17)) + MASK_DELTA;
    }

    /**
     * Return the crc whose masked representation is masked_crc.
     */
    public static int unmaskChecksum(int maskedCrc)
    {
        int rot = maskedCrc - MASK_DELTA;
        return ((rot >>> 17) | (rot << 15));
    }
}
