package org.iq80.leveldb.util;

import java.io.Closeable;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;

import sun.nio.ch.DirectBuffer;
import sun.misc.Unsafe;

import org.iq80.leveldb.Deallocator;
import org.iq80.leveldb.MemoryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.UnsignedBytes;

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
public final class ByteBuffers {
  private static final Logger LOGGER = LoggerFactory.getLogger(ByteBuffers.class);

  private ByteBuffers() {}

  interface BufferUtil {
    int calculateSharedBytes(ByteBuffer leftKey, ByteBuffer rightKey);

    int compare(ByteBuffer buffer1, int offset1, int length1, ByteBuffer buffer2, int offset2,
        int length2);

    ByteBufferCrc32C crc32c();
  }

  private static final BufferUtil UTIL = getBestUtil();

  /**
   * Returns the Unsafe-using util, or falls back to the pure-Java implementation if unable to do
   * so.
   */
  private static BufferUtil getBestUtil() {
    final String UNSAFE_UTIL_NAME = ByteBuffers.class.getName() + "$UnsafeUtil";
    try {
      BufferUtil util = (BufferUtil) Class.forName(UNSAFE_UTIL_NAME).getEnumConstants()[0];
      LOGGER.debug("Successfully loaded unsafe util");
      return util;
    } catch (Throwable t) {
      LOGGER.debug("Failed to load unsafe util, falling back to java impl ({})", t);
      return PureJavaUtil.INSTANCE;
    }
  }

  private enum PureJavaUtil implements BufferUtil {
    INSTANCE;

    @Override
    public int calculateSharedBytes(ByteBuffer leftKey, ByteBuffer rightKey) {
      int sharedKeyBytes = 0;
      final int lpos = leftKey.position(), rpos = rightKey.position();
      int maxSharedKeyBytes = Math.min(leftKey.remaining(), rightKey.remaining());
      while (sharedKeyBytes < maxSharedKeyBytes
          && leftKey.get(lpos + sharedKeyBytes) == rightKey.get(rpos + sharedKeyBytes)) {
        sharedKeyBytes++;
      }

      return sharedKeyBytes;
    }

    @Override
    public int compare(ByteBuffer buffer1, int offset1, int length1, ByteBuffer buffer2,
        int offset2, int length2) {
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
    public ByteBufferCrc32C crc32c() {
      return new PureJavaCrc32C();
    }
  }

  /**
   * parts of this code are borrowed and adapted from Guava's {@link UnsignedBytes} and Apache's
   * <a href=
   * "https://svn.apache.org/repos/asf/cassandra/trunk/src/java/org/apache/cassandra/utils/FastByteComparisons.java"
   * >FastByteComparisons</a>
   */
  @SuppressWarnings("unused")
  private enum UnsafeUtil implements BufferUtil {
    INSTANCE;

    private static final Unsafe unsafe;

    private static final long DIRECT_ADDRESS_FIELD_OFFSET;
    private static final long BYTE_ARRAY_FIELD_OFFSET;
    private static final long BYTE_ARRAY_OFFSET_FIELD_OFFSET;
    private static final boolean BIG_ENDIAN = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

    static {
      unsafe = (Unsafe) AccessController.doPrivileged(new PrivilegedAction<Object>() {
        @Override
        public Object run() {
          try {
            final Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return f.get(null);
          } catch (final NoSuchFieldException | IllegalAccessException e) {
            // It doesn't matter what we throw;
            // it's swallowed in get().
            throw new Error(e);
          }
        }
      });

      // sanity check - this should never fail
      if (unsafe.arrayIndexScale(byte[].class) != 1) {
        throw new AssertionError();
      }

      try {
        // access to private fields (and hb for read only buffers)
        DIRECT_ADDRESS_FIELD_OFFSET = getFieldOffset(Buffer.class, "address");
        BYTE_ARRAY_FIELD_OFFSET = getFieldOffset(ByteBuffer.class, "hb");
        BYTE_ARRAY_OFFSET_FIELD_OFFSET = getFieldOffset(ByteBuffer.class, "offset");
      } catch (NoSuchFieldException | SecurityException e) {
        throw new Error(e);
      }
    }

    private static long getFieldOffset(final Class<?> clazz, final String fieldName)
        throws NoSuchFieldException, SecurityException {
      final Field field = clazz.getDeclaredField(fieldName);
      field.setAccessible(true);
      return unsafe.objectFieldOffset(field);
    }

    private static Object byteArray(ByteBuffer b) {
      return unsafe.getObject(b, BYTE_ARRAY_FIELD_OFFSET);
    }

    private static long byteAddress(ByteBuffer b) {
      return unsafe.getInt(b, BYTE_ARRAY_OFFSET_FIELD_OFFSET) + Unsafe.ARRAY_BYTE_BASE_OFFSET;
    }

    private static long directAddress(ByteBuffer b) {
      return unsafe.getLong(b, DIRECT_ADDRESS_FIELD_OFFSET);
    }

    @Override
    public int calculateSharedBytes(ByteBuffer left, ByteBuffer right) {
      int maxSharedKeyBytes = Math.min(left.remaining(), right.remaining());
      if (left.isDirect()) {
        if (right.isDirect()) {
          return sharedDirectDirect(left.position() + directAddress(left),
              right.position() + directAddress(right), maxSharedKeyBytes);
        } else {
          return sharedArrayDirect(byteArray(right), right.position() + byteAddress(right),
              left.position() + directAddress(left), maxSharedKeyBytes);
        }
      } else if (right.isDirect()) {
        return sharedArrayDirect(byteArray(left), left.position() + byteAddress(left),
            right.position() + directAddress(right), maxSharedKeyBytes);
      }
      return sharedArrayArray(byteArray(left), left.position() + byteAddress(left),
          byteArray(right), right.position() + byteAddress(right), maxSharedKeyBytes);
    }

    private static int sharedArrayArray(Object arr1, long address1, Object arr2, long address2,
        int len) {
      int shared = 0;
      while (len > 7) {
        final long diff =
            unsafe.getLong(arr1, address1 + shared) ^ unsafe.getLong(arr2, address2 + shared);
        if (diff != 0) {
          return shared + ((BIG_ENDIAN ? Long.numberOfLeadingZeros(diff)
              : Long.numberOfTrailingZeros(diff)) >>> 3);
        }
        shared += 8;
        len -= 8;
      }

      while (len > 3) {
        final int diff =
            unsafe.getInt(arr1, address1 + shared) ^ unsafe.getInt(arr2, address2 + shared);
        if (diff != 0) {
          return shared + ((BIG_ENDIAN ? Integer.numberOfLeadingZeros(diff)
              : Integer.numberOfTrailingZeros(diff)) >>> 3);
        }
        shared += 4;
        len -= 4;
      }

      switch (len) {
        // @formatter:off
        case 3: if(unsafe.getByte(arr1, address1+shared) != unsafe.getByte(arr2, address2+shared)) return shared; else shared++;
        case 2: if(unsafe.getByte(arr1, address1+shared) != unsafe.getByte(arr2, address2+shared)) return shared; else shared++;
        case 1: if(unsafe.getByte(arr1, address1+shared) != unsafe.getByte(arr2, address2+shared)) return shared; else shared++;
        // @formatter:on
        default:
      }
      return shared;
    }

    private static int sharedArrayDirect(Object arr1, long address1, long address2, int len) {
      int shared = 0;
      while (len > 7) {
        final long diff =
            unsafe.getLong(arr1, address1 + shared) ^ unsafe.getLong(address2 + shared);
        if (diff != 0) {
          return shared + ((BIG_ENDIAN ? Long.numberOfLeadingZeros(diff)
              : Long.numberOfTrailingZeros(diff)) >>> 3);
        }
        shared += 8;
        len -= 8;
      }

      while (len > 3) {
        final int diff = unsafe.getInt(arr1, address1 + shared) ^ unsafe.getInt(address2 + shared);
        if (diff != 0) {
          return shared + ((BIG_ENDIAN ? Integer.numberOfLeadingZeros(diff)
              : Integer.numberOfTrailingZeros(diff)) >>> 3);
        }
        shared += 4;
        len -= 4;
      }

      switch (len) {
        // @formatter:off
        case 3: if(unsafe.getByte(arr1, address1+shared) != unsafe.getByte(address2+shared)) return shared; else shared++;
        case 2: if(unsafe.getByte(arr1, address1+shared) != unsafe.getByte(address2+shared)) return shared; else shared++;
        case 1: if(unsafe.getByte(arr1, address1+shared) != unsafe.getByte(address2+shared)) return shared; else shared++;
        // @formatter:on
        default:
      }
      return shared;
    }

    private static int sharedDirectDirect(long address1, long address2, int len) {
      int shared = 0;
      while (len > 7) {
        final long diff = unsafe.getLong(address1 + shared) ^ unsafe.getLong(address2 + shared);
        if (diff != 0) {
          return shared + ((BIG_ENDIAN ? Long.numberOfLeadingZeros(diff)
              : Long.numberOfTrailingZeros(diff)) >>> 3);
        }
        shared += 8;
        len -= 8;
      }

      while (len > 3) {
        final int diff = unsafe.getInt(address1 + shared) ^ unsafe.getInt(address2 + shared);
        if (diff != 0) {
          return shared + ((BIG_ENDIAN ? Integer.numberOfLeadingZeros(diff)
              : Integer.numberOfTrailingZeros(diff)) >>> 3);
        }
        shared += 4;
        len -= 4;
      }

      switch (len) {
        // @formatter:off
        case 3: if(unsafe.getByte(address1+shared) != unsafe.getByte(address2+shared)) return shared; else shared++;
        case 2: if(unsafe.getByte(address1+shared) != unsafe.getByte(address2+shared)) return shared; else shared++;
        case 1: if(unsafe.getByte(address1+shared) != unsafe.getByte(address2+shared)) return shared; else shared++;
        // @formatter:on
        default:
      }
      return shared;
    }

    @Override
    public int compare(ByteBuffer buffer1, int offset1, int length1, ByteBuffer buffer2,
        int offset2, int length2) {
      if (buffer1.isDirect()) {
        if (buffer2.isDirect()) {
          return compareDirectDirect(offset1 + directAddress(buffer1), length1,
              offset2 + directAddress(buffer2), length2);
        } else {
          return -compareArrayDirect(byteArray(buffer2), offset2 + byteAddress(buffer2), length2,
              offset1 + directAddress(buffer1), length1);
        }
      } else if (buffer2.isDirect()) {
        return compareArrayDirect(byteArray(buffer1), offset1 + byteAddress(buffer1), length1,
            offset2 + directAddress(buffer2), length2);
      }
      return compareArrayArray(byteArray(buffer1), offset1 + byteAddress(buffer1), length1,
          byteArray(buffer2), offset2 + byteAddress(buffer2), length2);
    }

    private static int compareArrayArray(Object arr1, long address1, int len1, Object arr2,
        long address2, int len2) {
      final int len = Math.min(len1, len2);
      int i;
      for (i = 0; i < (len & ~7); i += 8) {
        final long l = unsafe.getLong(arr1, address1 + i);
        final long r = unsafe.getLong(arr2, address2 + i);
        final long diff = l ^ r;
        if (diff != 0) {
          final int shift =
              (BIG_ENDIAN ? Long.numberOfLeadingZeros(diff) : Long.numberOfTrailingZeros(diff))
                  & ~7;
          return (int) (((l >>> shift) & 0xff) - ((r >>> shift) & 0xff));
        }
      }
      for (; i < (len & ~3); i += 4) {
        final int l = unsafe.getInt(arr1, address1 + i);
        final int r = unsafe.getInt(arr2, address2 + i);
        final int diff = l ^ r;
        if (diff != 0) {
          final int shift = (BIG_ENDIAN ? Integer.numberOfLeadingZeros(diff)
              : Integer.numberOfTrailingZeros(diff)) & ~7;
          return (int) (((l >>> shift) & 0xff) - ((r >>> shift) & 0xff));
        }
      }

      int result;
      switch (len - i) {
        // @formatter:off
        case 3: if((result = UnsignedBytes.compare(unsafe.getByte(arr1, address1+i), unsafe.getByte(arr2, address2+i++))) != 0) return result;
        case 2: if((result = UnsignedBytes.compare(unsafe.getByte(arr1, address1+i), unsafe.getByte(arr2, address2+i++))) != 0) return result;
        case 1: if((result = UnsignedBytes.compare(unsafe.getByte(arr1, address1+i), unsafe.getByte(arr2, address2+i++))) != 0) return result;
        // @formatter:on
        default:
      }
      return len1 - len2;
    }

    private static int compareArrayDirect(Object arr1, long address1, int len1, long address2,
        int len2) {
      final int len = Math.min(len1, len2);
      int i;
      for (i = 0; i < (len & ~7); i += 8) {
        final long l = unsafe.getLong(arr1, address1 + i);
        final long r = unsafe.getLong(address2 + i);
        final long diff = l ^ r;
        if (diff != 0) {
          final int shift =
              (BIG_ENDIAN ? Long.numberOfLeadingZeros(diff) : Long.numberOfTrailingZeros(diff))
                  & ~7;
          return (int) (((l >>> shift) & 0xff) - ((r >>> shift) & 0xff));
        }
      }
      for (; i < (len & ~3); i += 4) {
        final int l = unsafe.getInt(arr1, address1 + i);
        final int r = unsafe.getInt(address2 + i);
        final int diff = l ^ r;
        if (diff != 0) {
          final int shift = (BIG_ENDIAN ? Integer.numberOfLeadingZeros(diff)
              : Integer.numberOfTrailingZeros(diff)) & ~7;
          return (int) (((l >>> shift) & 0xff) - ((r >>> shift) & 0xff));
        }
      }

      int result;
      switch (len - i) {
        // @formatter:off
        case 3: if((result = UnsignedBytes.compare(unsafe.getByte(arr1, address1+i), unsafe.getByte(address2+i++))) != 0) return result;
        case 2: if((result = UnsignedBytes.compare(unsafe.getByte(arr1, address1+i), unsafe.getByte(address2+i++))) != 0) return result;
        case 1: if((result = UnsignedBytes.compare(unsafe.getByte(arr1, address1+i), unsafe.getByte(address2+i++))) != 0) return result;
        // @formatter:on
        default:
      }
      return len1 - len2;
    }

    private static int compareDirectDirect(long address1, int len1, long address2, int len2) {
      final int len = Math.min(len1, len2);
      int i;
      for (i = 0; i < (len & ~7); i += 8) {
        final long l = unsafe.getLong(address1 + i);
        final long r = unsafe.getLong(address2 + i);
        final long diff = l ^ r;
        if (diff != 0) {
          final int shift =
              (BIG_ENDIAN ? Long.numberOfLeadingZeros(diff) : Long.numberOfTrailingZeros(diff))
                  & ~7;
          return (int) (((l >>> shift) & 0xff) - ((r >>> shift) & 0xff));
        }
      }
      for (; i < (len & ~3); i += 4) {
        final int l = unsafe.getInt(address1 + i);
        final int r = unsafe.getInt(address2 + i);
        final int diff = l ^ r;
        if (diff != 0) {
          final int shift = (BIG_ENDIAN ? Integer.numberOfLeadingZeros(diff)
              : Integer.numberOfTrailingZeros(diff)) & ~7;
          return (int) (((l >>> shift) & 0xff) - ((r >>> shift) & 0xff));
        }
      }

      int result;
      switch (len - i) {
        // @formatter:off
        case 3: if((result = UnsignedBytes.compare(unsafe.getByte(address1+i), unsafe.getByte(address2+i++))) != 0) return result;
        case 2: if((result = UnsignedBytes.compare(unsafe.getByte(address1+i), unsafe.getByte(address2+i++))) != 0) return result;
        case 1: if((result = UnsignedBytes.compare(unsafe.getByte(address1+i), unsafe.getByte(address2+i++))) != 0) return result;
        // @formatter:on
        default:
      }
      return len1 - len2;
    }

    @Override
    public ByteBufferCrc32C crc32c() {
      return new UnsafeCrc32c();
    }

    private static class UnsafeCrc32c implements ByteBufferCrc32C {
      private int crc;

      public UnsafeCrc32c() {
        reset();
      }

      @Override
      public long getValue() {
        return (~crc) & 0xffffffffL;
      }

      public int getIntValue() {
        return ~crc;
      }

      @Override
      public void reset() {
        crc = 0xffffffff;
      }


      @Override
      final public void update(int b) {
        crc = (crc >>> 8) ^ T[T8_0_start + ((crc ^ b) & 0xff)];
      }

      @Override
      public void update(byte[] b, int off, int len) {
        crc = updateArray(crc, b, off + Unsafe.ARRAY_BYTE_BASE_OFFSET, len);
      }

      private static int updateArray(int localCrc, Object arr, long address, int len) {
        if (len > 7) {
          // first align to 8 bytes
          final int init = (int) (address & 7);
          switch (init) {
            // @formatter:off
            case 7: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(arr, address++)) & 0xff)];
            case 6: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(arr, address++)) & 0xff)];
            case 5: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(arr, address++)) & 0xff)];
            case 4: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(arr, address++)) & 0xff)];
            case 3: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(arr, address++)) & 0xff)];
            case 2: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(arr, address++)) & 0xff)];
            case 1: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(arr, address++)) & 0xff)];
            // @formatter:on
            default: // case 0, do nothing
          }
          len -= init;

          while (len > 7) {
            long c = unsafe.getLong(arr, address);
            if (BIG_ENDIAN) {
              // this could probably be done natively with the appropriate shifting.
              // but i'm lazy and this is intrinsified by hotspot so i'll call it close enough.
              // my apologies to everyone in big endian land
              c = Long.reverseBytes(c);
            }
            final int highlong = (int) (c >>> 32);
            localCrc ^= (int) c;
            // @formatter:off
            localCrc = (T[T8_7_start +  (localCrc         & 0xff)] ^ T[T8_6_start + ((localCrc >>>  8) & 0xff)]) ^
                       (T[T8_5_start + ((localCrc >>> 16) & 0xff)] ^ T[T8_4_start + ((localCrc >>> 24) & 0xff)]) ^
                       (T[T8_3_start +  (highlong         & 0xff)] ^ T[T8_2_start + ((highlong >>>  8) & 0xff)]) ^
                       (T[T8_1_start + ((highlong >>> 16) & 0xff)] ^ T[T8_0_start + ((highlong >>> 24) & 0xff)]);
            // @formatter:on

            address += 8;
            len -= 8;
          }
        }

        switch (len) {
          // @formatter:off
          case 7: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(arr, address++)) & 0xff)];
          case 6: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(arr, address++)) & 0xff)];
          case 5: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(arr, address++)) & 0xff)];
          case 4: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(arr, address++)) & 0xff)];
          case 3: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(arr, address++)) & 0xff)];
          case 2: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(arr, address++)) & 0xff)];
          case 1: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(arr, address++)) & 0xff)];
          // @formatter:on
          default: // case 0, do nothing
        }
        return localCrc;
      }

      private static int updateDirect(int localCrc, long address, int len) {
        if (len > 7) {
          // first align to 8 bytes
          final int init = (int) (address & 7);
          switch (init) {
            // @formatter:off
            case 7: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(address++)) & 0xff)];
            case 6: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(address++)) & 0xff)];
            case 5: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(address++)) & 0xff)];
            case 4: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(address++)) & 0xff)];
            case 3: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(address++)) & 0xff)];
            case 2: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(address++)) & 0xff)];
            case 1: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(address++)) & 0xff)];
            // @formatter:on
            default: // case 0, do nothing
          }
          len -= init;

          while (len > 7) {
            long c = unsafe.getLong(address);
            if (BIG_ENDIAN) {
              c = Long.reverseBytes(c);
            }
            final int highlong = (int) (c >>> 32);
            localCrc ^= (int) c;
            // @formatter:off
            localCrc = (T[T8_7_start +  (localCrc         & 0xff)] ^ T[T8_6_start + ((localCrc >>>  8) & 0xff)]) ^
                       (T[T8_5_start + ((localCrc >>> 16) & 0xff)] ^ T[T8_4_start + ((localCrc >>> 24) & 0xff)]) ^
                       (T[T8_3_start +  (highlong         & 0xff)] ^ T[T8_2_start + ((highlong >>>  8) & 0xff)]) ^
                       (T[T8_1_start + ((highlong >>> 16) & 0xff)] ^ T[T8_0_start + ((highlong >>> 24) & 0xff)]);
            // @formatter:on

            address += 8;
            len -= 8;
          }
        }

        switch (len) {
          // @formatter:off
          case 7: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(address++)) & 0xff)];
          case 6: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(address++)) & 0xff)];
          case 5: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(address++)) & 0xff)];
          case 4: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(address++)) & 0xff)];
          case 3: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(address++)) & 0xff)];
          case 2: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(address++)) & 0xff)];
          case 1: localCrc = (localCrc >>> 8) ^ T[T8_0_start + ((localCrc ^ unsafe.getByte(address++)) & 0xff)];
          // @formatter:on
          default: // case 0, do nothing
        }
        return localCrc;
      }


      @Override
      public void update(ByteBuffer b, int off, int len) {
        if (b.isDirect()) {
          crc = updateDirect(crc, off + directAddress(b), len);
        } else {
          crc = updateArray(crc, byteArray(b), off + byteAddress(b), len);
        }
      }
    }

  }

  public static final ByteBuffer EMPTY_BUFFER =
      ByteBuffer.allocate(0).order(ByteOrder.LITTLE_ENDIAN);

  public static final ByteBuffer ZERO64;

  static {
    ByteBuffer b = ByteBuffer.allocateDirect(64);
    while (b.hasRemaining()) {
      b.put((byte) 0);
    }
    b.flip();
    ZERO64 = b.asReadOnlyBuffer();
  }

  private static final boolean directBufferSupport;

  static {
    boolean success = false;
    try {
      ((DirectBuffer) ByteBuffer.allocateDirect(8)).cleaner().clean();
      success = true;
    } catch (Throwable t) {
      LOGGER.debug("failed to access DirectBuffer support", t);
      success = false;
    } finally {
      LOGGER.debug("DirectBuffer support:", success);
      directBufferSupport = success;
    }
  }

  public static void freeDirect(ByteBuffer buffer) {
    if (directBufferSupport && buffer.isDirect()) {
      ((DirectBuffer) buffer).cleaner().clean();
    }
    // else
    // leave it to Java GC
  }

  public static void writeLengthPrefixedBytes(ByteBuffer dst, ByteBuffer src) {
    VariableLengthQuantity.writeVariableLengthInt(src.remaining(), dst);
    dst.put(src);
  }

  public static int getRemaining(ByteBuffer[] bufs) {
    int size = 0;
    for (ByteBuffer buf : bufs) {
      size += buf.remaining();
    }
    return size;
  }

  public static void writeLengthPrefixedBytes(GrowingBuffer buffer, ByteBuffer src) {
    VariableLengthQuantity.writeVariableLengthInt(src.remaining(), buffer);
    buffer.put(src);
  }

  public static void writeLengthPrefixedBytesTransparent(GrowingBuffer buffer, ByteBuffer src) {
    src.mark();
    writeLengthPrefixedBytes(buffer, src);
    src.reset();
  }

  public static void writeLengthPrefixedBytesTransparent(ByteBuffer buffer, ByteBuffer src) {
    src.mark();
    writeLengthPrefixedBytes(buffer, src);
    src.reset();
  }

  public static void writeLengthPrefixedBytes(GrowingBuffer buffer, ByteBuffer[] srcs) {
    VariableLengthQuantity.writeVariableLengthInt(getRemaining(srcs), buffer);
    for (ByteBuffer src : srcs) {
      buffer.put(src);
    }
  }

  public static ByteBuffer readLengthPrefixedBytes(ByteBuffer src) {
    int length = VariableLengthQuantity.readVariableLengthInt(src);
    return duplicateAndAdvance(src, length);
  }

  /**
   * buffer duplication which preserves byte order
   */
  public static ByteBuffer duplicate(ByteBuffer src) {
    return src.duplicate().order(src.order());
  }

  public static ByteBuffer duplicate(ByteBuffer src, int position, int limit) {
    ByteBuffer ret = duplicate(src);
    ret.limit(limit).position(position);
    return ret;
  }

  public static ByteBuffer duplicateByLength(ByteBuffer src, int position, int length) {
    return duplicate(src, position, position + length);
  }

  public static ByteBuffer slice(ByteBuffer src) {
    return src.slice().order(src.order());
  }

  public static ByteBuffer readOnly(ByteBuffer src) {
    return src.asReadOnlyBuffer().order(src.order());
  }

  public static int readUnsignedByte(ByteBuffer src) {
    return src.get() & 0xFF;
  }

  public static int getUnsignedByte(ByteBuffer src, int position) {
    return src.get(position) & 0xFF;
  }

  public static ByteBuffer duplicateAndAdvance(ByteBuffer src, int length) {
    final int oldpos = src.position();
    src.position(oldpos + length);
    return duplicate(src, oldpos, oldpos + length);
  }

  public static int calculateSharedBytes(ByteBuffer leftKey, ByteBuffer rightKey) {
    if (leftKey == null || rightKey == null) {
      return 0;
    }
    return UTIL.calculateSharedBytes(leftKey, rightKey);
  }

  public static int compare(ByteBuffer a, ByteBuffer b) {
    return UTIL.compare(a, a.position(), a.remaining(), b, b.position(), b.remaining());
  }

  public static ByteBuffer copy(ByteBuffer src, int length, MemoryManager memory) {
    ByteBuffer ret = memory.allocate(length);
    ret.mark();
    ret.put(duplicateByLength(src, src.position(), length)).limit(ret.position()).reset();
    return ret;
  }

  public static ByteBuffer copy(ByteBuffer src, MemoryManager memory) {
    return copy(src, src.remaining(), memory);
  }
  
  public static ByteBuffer copyOwned(ByteBuffer src){
    ByteBuffer ret = ByteBuffer.allocate(src.remaining()).order(src.order());
    ret.put(src).flip();
    return ret;
  }

  public static ByteBuffer heapCopy(ByteBuffer src) {
    return copy(src, MemoryManagers.heap());
  }

  public static ByteBuffer copy(byte[] src, MemoryManager memory) {
    ByteBuffer ret = memory.allocate(src.length);
    ret.mark();
    ret.put(src).limit(ret.position()).reset();
    return ret;
  }

  public static byte[] toArray(ByteBuffer src, byte[] dst) {
    src.mark();
    src.get(dst);
    src.reset();
    return dst;
  }

  public static byte[] toArray(ByteBuffer src) {
    return toArray(src, new byte[src.remaining()]);
  }

  public static String toString(ByteBuffer src) {
    return new String(toArray(ByteBuffers.duplicate(src)), StandardCharsets.UTF_8);
  }

  /**
   * @return dst
   */
  public static ByteBuffer putLength(ByteBuffer dst, ByteBuffer src, int length) {
    int oldlim = src.limit();
    src.limit(src.position() + length);
    dst.put(src);
    src.limit(oldlim);
    return dst;
  }

  public static ByteBufferCrc32C crc32c() {
    return UTIL.crc32c();
  }

  private static final int MASK_DELTA = 0xa282ead8;

  /**
   * Return a masked representation of crc.
   * <p/>
   * Motivation: it is problematic to compute the CRC of a string that contains embedded CRCs.
   * Therefore we recommend that CRCs stored somewhere (e.g., in files) should be masked before
   * being stored.
   */
  public static int maskChecksum(int crc) {
    // Rotate right by 15 bits and add a constant.
    return ((crc >>> 15) | (crc << 17)) + MASK_DELTA;
  }

  /**
   * Return the crc whose masked representation is masked_crc.
   */
  public static int unmaskChecksum(int maskedCrc) {
    int rot = maskedCrc - MASK_DELTA;
    return ((rot >>> 17) | (rot << 15));
  }

  public static Closeable freer(ByteBuffer b, Deallocator m) {
    return new MemoryFreer(b, m);
  }

  private static class MemoryFreer implements Closeable {
    private final ByteBuffer buffer;
    private final Deallocator memory;

    public MemoryFreer(ByteBuffer buffer, Deallocator memory) {
      this.buffer = buffer;
      this.memory = memory;
    }

    @Override
    public void close() {
      memory.free(buffer);
    }
  }
}