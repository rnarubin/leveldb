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

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.iq80.leveldb.MemoryManager;
import org.iq80.leveldb.util.ByteBuffers.BufferUtil;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.base.Strings;

import static java.nio.charset.StandardCharsets.US_ASCII;

public abstract class BufferUtilTest
{

    protected final BufferUtil util = getUtil();

    @Test
    public void testCompare()
    {
        testCompare("", "", 0);
        testCompare("a", "", 1);
        testCompare("a", "b", -1);
        testCompare("abcd", "", 4);
        testCompare("abcd", "abcdefg", -3);

        testCompare("abcdefgh", "zbcdefgh", 'a' - 'z');
        testCompare("abcdefgh", "aqcdefgh", 'b' - 'q');
        testCompare("abcdefgh", "abtdefgh", 'c' - 't');
        testCompare("abcdefgh", "abcjefgh", 'd' - 'j');
        testCompare("abcdefgh", "abcdbfgh", 'e' - 'b');
        testCompare("abcdefgh", "abcdewgh", 'f' - 'w');
        testCompare("abcdefgh", "abcdef&h", 'g' - '&');
        testCompare("abcdefgh", "abcdefgg", 'h' - 'g');

        testCompare("abcdefghijkl", "abcdefghijml", 'k' - 'm');

        testCompare("00abcdefghijkl", "00000abc$efghijkl", 2, 6, 5, 9, 'd' - '$');
        testCompare("00abcdefghijkl", "00000abc$efghijkl", 7, 4, 10, 4, 0);
        testCompare(Strings.repeat("0", 15) + Strings.repeat("abcdefghijkl", 567),
                Strings.repeat("0", 73) + Strings.repeat("abcdefghijkl", 678), 15, 12 * 567, 73, 12 * 567, 0);
        testCompare(Strings.repeat("0", 15) + Strings.repeat("abcdefghijkl", 567) + "x", Strings.repeat("0", 73)
                + Strings.repeat("abcdefghijkl", 678), 15, 12 * 567 + 1, 73, 12 * 567 + 5, 'x' - 'a');
        testCompare(Strings.repeat("0", 15) + Strings.repeat("abcdefghijkl", 567),
                Strings.repeat("0", 73) + Strings.repeat("abcdefghijkl", 678), 15, 12 * 567, 73, 12 * 678, (12 * 567)
                        - (12 * 678));
    }

    private void testCompare(String a, String b, int expected)
    {
        testCompare(a, b, 0, a.length(), 0, b.length(), expected);
    }

    private void testCompare(String a, String b, int o1, int l1, int o2, int l2, int expected)
    {
        MemoryManager heap = MemoryManagers.heap(), direct = MemoryManagers.direct();
        Assert.assertEquals(util.compare(buf(a, heap), o1, l1, buf(b, heap), o2, l2), expected);
        Assert.assertEquals(util.compare(buf(b, heap), o2, l2, buf(a, heap), o1, l1), -expected);
        Assert.assertEquals(util.compare(buf(a, direct), o1, l1, buf(b, heap), o2, l2), expected);
        Assert.assertEquals(util.compare(buf(b, direct), o2, l2, buf(a, heap), o1, l1), -expected);
        Assert.assertEquals(util.compare(buf(a, heap), o1, l1, buf(b, direct), o2, l2), expected);
        Assert.assertEquals(util.compare(buf(b, heap), o2, l2, buf(a, direct), o1, l1), -expected);
        Assert.assertEquals(util.compare(buf(a, direct), o1, l1, buf(b, direct), o2, l2), expected);
        Assert.assertEquals(util.compare(buf(b, direct), o2, l2, buf(a, direct), o1, l1), -expected);
    }

    @Test
    public void testSharedBytes()
    {
        testSharedBytes("", "", 0);
        testSharedBytes("abcd", "efgh", 0);
        testSharedBytes("abcd", "", 0);
        testSharedBytes("abcd", "a", 1);

        testSharedBytes("abcd", "afgh", 1);
        testSharedBytes("abcd", "abgh", 2);
        testSharedBytes("abcd", "abch", 3);
        testSharedBytes("abcd", "abcd", 4);

        testSharedBytes("123456789", "1234_abcde", 4);
        testSharedBytes("123456789", "12345abcde", 5);
        testSharedBytes("123456789", "123456bcde", 6);
        testSharedBytes("123456789", "1234567cde", 7);
        testSharedBytes("123456789", "12345678de", 8);

        testSharedBytes("qwertyuiopasdfghjkl1234", "qwertyuiopasdfghjkl5678", 19);
        testSharedBytes(Strings.repeat("abcdefg", 900), Strings.repeat("abcdefg", 900), 7 * 900);
        testSharedBytes(Strings.repeat("abcdefg", 900) + "1", Strings.repeat("abcdefg", 900), 7 * 900);
        testSharedBytes(Strings.repeat("abcdefg", 532), Strings.repeat("abcdefg", 900), 7 * 532);
    }

    private void testSharedBytes(String a, String b, int expected)
    {
        MemoryManager heap = MemoryManagers.heap(), direct = MemoryManagers.direct();
        Assert.assertEquals(util.calculateSharedBytes(buf(a, heap), buf(b, heap)), expected);
        Assert.assertEquals(util.calculateSharedBytes(buf(b, heap), buf(a, heap)), expected);
        Assert.assertEquals(util.calculateSharedBytes(buf(a, direct), buf(b, heap)), expected);
        Assert.assertEquals(util.calculateSharedBytes(buf(b, direct), buf(a, heap)), expected);
        Assert.assertEquals(util.calculateSharedBytes(buf(a, heap), buf(b, direct)), expected);
        Assert.assertEquals(util.calculateSharedBytes(buf(b, heap), buf(a, direct)), expected);
        Assert.assertEquals(util.calculateSharedBytes(buf(a, direct), buf(b, direct)), expected);
        Assert.assertEquals(util.calculateSharedBytes(buf(b, direct), buf(a, direct)), expected);
    }

    @Test
    public void testPutZero()
    {
        testPutZero(MemoryManagers.heap());
        testPutZero(MemoryManagers.direct());
    }

    private void testPutZero(MemoryManager memory)
    {
        {
            MemoryManager bm = bigEndianWrapper(memory);
            final ByteBuffer b = bm.allocate(8).putLong(0xdeadbeef12345678L);
            b.rewind();

            {
                ByteBuffer dst = ByteBuffers.copy(b, bm);
                util.putZero(dst, 2);
                Assert.assertEquals(dst.position(), 2);
                dst.rewind();
                Assert.assertEquals(dst.getLong(), 0x0000beef12345678L);
            }
            {
                ByteBuffer dst = ByteBuffers.copy(b, bm);
                dst.position(5);
                util.putZero(dst, 1);
                Assert.assertEquals(dst.position(), 6);
                dst.rewind();
                Assert.assertEquals(dst.getLong(), 0xdeadbeef12005678L);
            }
            {
                ByteBuffer dst = ByteBuffers.copy(b, bm);
                dst.position(3);
                util.putZero(dst, 5);
                Assert.assertEquals(dst.position(), 8);
                dst.rewind();
                Assert.assertEquals(dst.getLong(), 0xdeadbe0000000000L);
            }
            bm.free(b);
        }
        {
            ByteBuffer b = memory.allocate(12345);
            while (b.hasRemaining()) {
                b.put((byte) 0xff);
            }
            b.rewind();
            b.position(17);
            util.putZero(b, 53);
            b.position(167);
            util.putZero(b, 391);
            b.position(1000);
            util.putZero(b, 11300);

            for (int i = 0; i < 17; i++) {
                Assert.assertEquals((byte) 0xff, b.get(i));
            }
            for (int i = 17; i < 17 + 53; i++) {
                Assert.assertEquals((byte) 0x00, b.get(i));
            }
            for (int i = 17 + 53; i < 167; i++) {
                Assert.assertEquals((byte) 0xff, b.get(i));
            }
            for (int i = 167; i < 167 + 391; i++) {
                Assert.assertEquals((byte) 0x00, b.get(i));
            }
            for (int i = 167 + 391; i < 1000; i++) {
                Assert.assertEquals((byte) 0xff, b.get(i));
            }
            for (int i = 1000; i < 1000 + 11300; i++) {
                Assert.assertEquals((byte) 0x00, b.get(i));
            }
            for (int i = 1000 + 11300; i < b.limit(); i++) {
                Assert.assertEquals((byte) 0xff, b.get(i));
            }
            memory.free(b);
        }
    }

    @Test(dataProvider = "crcs")
    public void testCrc(int expectedCrc, byte[] data)
    {
        for (ByteBuffer b : heapAndDirect(data)) {
            Assert.assertEquals(expectedCrc, computeCrc(b));
        }
    }

    @DataProvider(name = "crcs")
    public Object[][] data()
    {
        return new Object[][] {
                new Object[] {0x8a9136aa, arrayOf(32, (byte) 0)},
                new Object[] {0x62a8ab43, arrayOf(32, (byte) 0xff)},
                new Object[] {0x46dd794e, arrayOf(32, new Function<Integer, Byte>()
                {
                    @Override
                    public Byte apply(Integer position)
                    {
                        return (byte) position.intValue();
                    }
                })},
                new Object[] {0x113fdb5c, arrayOf(32, new Function<Integer, Byte>()
                {
                    @Override
                    public Byte apply(Integer position)
                    {
                        return (byte) (31 - position);
                    }
                })},
                new Object[] {0xd9963a56, arrayOf(new int[] {
                        0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
                        0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})},

                new Object[] { 0xc1d04330, "a".getBytes(US_ASCII) },
                new Object[] { 0xe2a22936, "ab".getBytes(US_ASCII) },
                new Object[] { 0x364b3fb7, "abc".getBytes(US_ASCII) },
                new Object[] { 0x92c80a31, "abcd".getBytes(US_ASCII) },
                new Object[] { 0xc450d697, "abcde".getBytes(US_ASCII) },
                new Object[] { 0x53bceff1, "abcdef".getBytes(US_ASCII) },
                new Object[] { 0xe627f441, "abcdefg".getBytes(US_ASCII) },
                new Object[] { 0xe443cace, "abcdefghijklmnopqrstuv".getBytes(US_ASCII) },
                new Object[] { 0x201d71a2, Strings.repeat("abcdefghijklmnopqrstuv", 50).getBytes(US_ASCII) },
                new Object[] { 0x32612e7e, Strings.repeat("abcdefghijklmnopqrstuv", 500).getBytes(US_ASCII) },
        };
    }

    @Test
    public void testProducesDifferentCrcs()
            throws UnsupportedEncodingException
    {
        ByteBuffer[] a = heapAndDirect("a".getBytes("ASCII"));
        ByteBuffer[] b = heapAndDirect("foo".getBytes("ASCII"));

        Assert.assertFalse(computeCrc(a[0]) == computeCrc(b[0]));
        Assert.assertFalse(computeCrc(a[1]) == computeCrc(b[1]));
    }

    @Test
    public void testComposes()
            throws UnsupportedEncodingException
    {
        ByteBuffer[] a = heapAndDirect("hello ".getBytes("ASCII"));
        ByteBuffer[] b = heapAndDirect("world".getBytes("ASCII"));
        ByteBuffer[] c = heapAndDirect("hello world".getBytes("ASCII"));

        {
            ByteBufferCrc32 crc = util.crc32();
            crc.update(a[0], 0, 6);
            crc.update(b[0], 0, 5);

            Assert.assertEquals(crc.getIntValue(), computeCrc(c[0]));
        }

        {
            ByteBufferCrc32 crc = util.crc32();
            crc.update(a[1], 0, 6);
            crc.update(b[1], 0, 5);

            Assert.assertEquals(crc.getIntValue(), computeCrc(c[1]));
        }
    }

    private int computeCrc(ByteBuffer data)
    {
        ByteBufferCrc32 crc = util.crc32();
        crc.update(data, data.position(), data.remaining());
        return crc.getIntValue();
    }
    
    private static ByteBuffer[] heapAndDirect(byte[] data)
    {
        List<ByteBuffer> bufs = new ArrayList<>();
        bufs.add((ByteBuffer) MemoryManagers.heap().allocate(data.length).put(data).flip());
        bufs.add((ByteBuffer) MemoryManagers.direct().allocate(data.length).put(data).flip());
        for (MemoryManager memory : Arrays.asList(MemoryManagers.heap(), MemoryManagers.direct())) {
            for (int i = 1; i <= 16; i++) {
                ByteBuffer b = memory.allocate(data.length + i);
                b.position(i);
                b.put(data);
                b.position(i);
                bufs.add(b);
            }
        }
        return bufs.toArray(new ByteBuffer[bufs.size()]);
    }

    private static byte[] arrayOf(int size, byte value)
    {
        byte[] result = new byte[size];
        Arrays.fill(result, value);
        return result;
    }

    @SuppressWarnings("ConstantConditions")
    private static byte[] arrayOf(int size, Function<Integer, Byte> generator)
    {
        byte[] result = new byte[size];
        for (int i = 0; i < result.length; ++i) {
            result[i] = generator.apply(i);
        }

        return result;
    }

    private static byte[] arrayOf(int[] bytes)
    {
        byte[] result = new byte[bytes.length];
        for (int i = 0; i < result.length; ++i) {
            result[i] = (byte) bytes[i];
        }

        return result;
    }

    private MemoryManager bigEndianWrapper(final MemoryManager memory)
    {
        return new MemoryManager()
        {
            @Override
            public ByteBuffer allocate(int capacity)
            {
                return memory.allocate(capacity).order(ByteOrder.BIG_ENDIAN);
            }

            @Override
            public void free(ByteBuffer buffer)
            {
                memory.free(buffer);
            }
        };
    }

    private static ByteBuffer buf(String s, MemoryManager memory)
    {
        ByteBuffer ret = memory.allocate(s.length()).put(s.getBytes(StandardCharsets.UTF_8));
        ret.flip();
        return ret;
    }

    protected abstract BufferUtil getUtil();

    public static class PureJavaUtilTest
            extends BufferUtilTest
    {
        private final static BufferUtil util;
        static {
            try {
                util = (BufferUtil) Class.forName("org.iq80.leveldb.util.ByteBuffers$PureJavaUtil").getEnumConstants()[0];
            }
            catch (ClassNotFoundException e) {
                throw new Error(e);
            }
        }
        @Override
        protected BufferUtil getUtil()
        {
            return util;
        }
    }

    public static class UnsafeUtilTest
            extends BufferUtilTest
    {
        private final static BufferUtil util;
        static {
            try {
                util = (BufferUtil) Class.forName("org.iq80.leveldb.util.ByteBuffers$UnsafeUtil").getEnumConstants()[0];
            }
            catch (ClassNotFoundException e) {
                throw new Error(e);
            }
        }

        @Override
        protected BufferUtil getUtil()
        {
            return util;
        }
    }

    /**
     * test as what's actually called in the code
     */
    public static class FrontEndUtilTest
            extends BufferUtilTest
    {
        private static final BufferUtil util = new BufferUtil()
        {
            @Override
            public int calculateSharedBytes(ByteBuffer leftKey, ByteBuffer rightKey)
            {
                return ByteBuffers.calculateSharedBytes(leftKey, rightKey);
            }

            @Override
            public void putZero(ByteBuffer dst, int length)
            {
                ByteBuffers.putZero(dst, length);
            }

            @Override
            public int compare(ByteBuffer buffer1,
                    int offset1,
                    int length1,
                    ByteBuffer buffer2,
                    int offset2,
                    int length2)
            {
                return ByteBuffers.compare(ByteBuffers.duplicate(buffer1, offset1, offset1 + length1),
                        ByteBuffers.duplicate(buffer2, offset2, offset2 + length2));
            }

            @Override
            public ByteBufferCrc32 crc32()
            {
                return ByteBuffers.crc32();
            }
        };

        @Override
        protected BufferUtil getUtil()
        {
            return util;
        }
    }
}

