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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.iq80.leveldb.Compression;
import org.iq80.leveldb.DBException;

/**
 * <p>
 * A Snappy abstraction which attempts uses the iq80 implementation and falls back
 * to the xerial Snappy implementation it cannot be loaded.  You can change the
 * load order by setting the 'leveldb.snappy' system property.  Example:
 * <p/>
 * <code>
 * -Dleveldb.snappy=xerial,iq80
 * </code>
 * <p/>
 * The system property can also be configured with the name of a class which
 * implements the Snappy.SPI interface.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public final class Snappy
{
    private Snappy()
    {
    }

    public interface SPI
            extends Compression
    {
        byte[] compress(String text)
                throws IOException;
    }

    public static class XerialSnappy
            implements SPI
    {
        // xerial snappy requires either double direct buffers or double arrays for [un]compression.
        // cache a spare buffer to swap in if the given pairs are mismatched
        private static final ThreadLocal<ByteBuffer> scratch = new ThreadLocal<ByteBuffer>(){
            @Override
            public synchronized ByteBuffer initialValue()
            {
                return ByteBuffer.allocateDirect(4096);
            }
        };

        private static ByteBuffer getDirectBuffer(int size)
        {
            ByteBuffer buf = scratch.get();
            if (buf.capacity() < size) {
                ByteBuffers.freeDirect(buf);
                buf = ByteBuffer.allocateDirect(size);
                scratch.set(buf);
            }
            buf.clear();
            return buf;
        }

        static {
            // Make sure that the JNI libs are fully loaded.
            try {
                org.xerial.snappy.Snappy.compress("test");
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public int compress(ByteBuffer uncompressed, ByteBuffer compressed)
        {
            assert !compressed.isReadOnly();
            try {
                if (uncompressed.isDirect()) {
                    if (compressed.isDirect()) {
                        return org.xerial.snappy.Snappy.compress(uncompressed, compressed);
                    }
                    else{
                        ByteBuffer direct = getDirectBuffer(maxCompressedLength(uncompressed));
                        final int length = org.xerial.snappy.Snappy.compress(uncompressed, direct);
                        compressed.mark();
                        compressed.put(direct).reset().limit(compressed.position() + length);
                        return length;
                    }
                }
                else {
                    assert !uncompressed.isReadOnly(); // this i prohibit just because it's another permutation that would make my life more difficult (the backing array, if any, is inaccessible)
                    if (compressed.isDirect()) {
                        ByteBuffer direct = getDirectBuffer(uncompressed.remaining());
                        uncompressed.mark();
                        direct.put(uncompressed).flip();
                        uncompressed.reset();
                        return org.xerial.snappy.Snappy.compress(direct, compressed);
                    }
                    else {
                        return org.xerial.snappy.Snappy.compress(uncompressed.array(), uncompressed.arrayOffset()
                                + uncompressed.position(), uncompressed.remaining(), compressed.array(),
                                compressed.arrayOffset() + compressed.position());
                    }
                }
            }
            catch (IOException e) {
                throw new DBException(e);
            }
        }

        @Override
        public int uncompress(ByteBuffer compressed, ByteBuffer uncompressed)
        {
            assert !compressed.isReadOnly();
            try {
                if (compressed.isDirect()) {
                    if (uncompressed.isDirect()) {
                        return org.xerial.snappy.Snappy.uncompress(compressed, uncompressed);
                    }
                    else {
                        ByteBuffer direct = getDirectBuffer(maxUncompressedLength(compressed));
                        final int length = org.xerial.snappy.Snappy.compress(compressed, direct);
                        uncompressed.mark();
                        uncompressed.put(direct).reset().limit(uncompressed.position() + length);
                        return length;
                    }
                }
                else {
                    assert !uncompressed.isReadOnly();
                    if (uncompressed.isDirect()) {
                        ByteBuffer direct = getDirectBuffer(compressed.remaining());
                        compressed.mark();
                        direct.put(compressed).flip();
                        compressed.reset();
                        return org.xerial.snappy.Snappy.uncompress(direct, uncompressed);
                    }
                    else {
                        return org.xerial.snappy.Snappy.uncompress(compressed.array(), compressed.arrayOffset()
                                + compressed.position(), compressed.remaining(), uncompressed.array(),
                                uncompressed.arrayOffset() + uncompressed.position());
                    }
                }
            }
            catch (IOException e) {
                throw new DBException(e);
            }
        }

        @Override
        public byte[] compress(String text)
                throws IOException
        {
            return org.xerial.snappy.Snappy.compress(text);
        }

        @Override
        public int maxUncompressedLength(ByteBuffer compressed)
        {
            return Snappy.maxUncompressedLength(compressed);
        }

        @Override
        public int maxCompressedLength(ByteBuffer buf)
        {
            return org.xerial.snappy.Snappy.maxCompressedLength(buf.remaining());
        }

        @Override
        public byte persistentId()
        {
            return 1;
        }
    }

    /*
    public static class IQ80Snappy
            implements SPI
    {
        static {
            // Make sure that the library can fully load.
            try {
                new IQ80Snappy().compress("test");
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public int uncompress(ByteBuffer compressed, ByteBuffer uncompressed)
        {
            byte[] input;
            int inputOffset;
            int length;
            byte[] output;
            int outputOffset;
            if (compressed.hasArray()) {
                input = compressed.array();
                inputOffset = compressed.arrayOffset() + compressed.position();
                length = compressed.remaining();
            }
            else {
                input = new byte[compressed.remaining()];
                inputOffset = 0;
                length = input.length;
                compressed.mark();
                compressed.get(input);
                compressed.reset();
            }
            if (uncompressed.hasArray()) {
                output = uncompressed.array();
                outputOffset = uncompressed.arrayOffset() + uncompressed.position();
            }
            else {
                int t = org.iq80.snappy.Snappy.getUncompressedLength(input, inputOffset);
                output = new byte[t];
                outputOffset = 0;
            }

            int count = org.iq80.snappy.Snappy.uncompress(input, inputOffset, length, output, outputOffset);
            if (uncompressed.hasArray()) {
                uncompressed.limit(uncompressed.position() + count);
            }
            else {
                int p = uncompressed.position();
                uncompressed.limit(uncompressed.capacity());
                uncompressed.put(output, 0, count);
                uncompressed.flip().position(p);
            }
            return count;
        }

        @Override
        public int compress(ByteBuffer uncompressed, ByteBuffer compressed)
                throws IOException
        {
            return org.iq80.snappy.Snappy.uncompress(input, inputOffset, length, output, outputOffset);
        }

        @Override
        public byte[] compress(String text)
                throws IOException
        {
            byte[] uncomressed = text.getBytes("UTF-8");
            byte[] compressedOut = new byte[maxCompressedLength(uncomressed.length)];
            int compressedSize = compress(uncomressed, 0, uncomressed.length, compressedOut, 0);
            byte[] trimmedBuffer = new byte[compressedSize];
            System.arraycopy(compressedOut, 0, trimmedBuffer, 0, compressedSize);
            return trimmedBuffer;
        }

        @Override
        public int maxCompressedLength(int length)
        {
            return org.iq80.snappy.Snappy.maxCompressedLength(length);
        }

        @Override
        public int maxUncompressedLength(ByteBuffer compressed)
        {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public int maxCompressedLength(ByteBuffer uncompressed)
        {
            // TODO Auto-generated method stub
            return 0;

    private static int maxCompressedLength(int length)
    {
        // Compressed data can be defined as:
        //    compressed := item* literal*
        //    item       := literal* copy
        //
        // The trailing literal sequence has a space blowup of at most 62/60
        // since a literal of length 60 needs one tag byte + one extra byte
        // for length information.
        //
        // Item blowup is trickier to measure.  Suppose the "copy" op copies
        // 4 bytes of data.  Because of a special check in the encoding code,
        // we produce a 4-byte copy only if the offset is < 65536.  Therefore
        // the copy op takes 3 bytes to encode, and this type of item leads
        // to at most the 62/60 blowup for representing literals.
        //
        // Suppose the "copy" op copies 5 bytes of data.  If the offset is big
        // enough, it will take 5 bytes to encode the copy op.  Therefore the
        // worst case here is a one-byte literal followed by a five-byte copy.
        // I.e., 6 bytes of input turn into 7 bytes of "compressed" data.
        //
        // This last factor dominates the blowup, so the final estimate is:
        return 32 + length + (length / 6);
    }
        }

        @Override
        public byte persistentId()
        {
            // TODO Auto-generated method stub
            return 0;
        }
    }*/

    private static final SPI SNAPPY;

    static {
        SPI attempt = null;
        String[] factories = System.getProperty("leveldb.snappy", "xerial,iq80").split(",");
        for (int i = 0; i < factories.length && attempt == null; i++) {
            String name = factories[i];
            try {
                name = name.trim();
                if ("xerial".equals(name.toLowerCase())) {
                    name = "org.iq80.leveldb.util.Snappy$XerialSnappy";
                }
                else if ("iq80".equals(name.toLowerCase())) {
                    name = "org.iq80.leveldb.util.Snappy$IQ80Snappy";
                }
                attempt = (SPI) Thread.currentThread().getContextClassLoader().loadClass(name).newInstance();
            }
            catch (Throwable e) {
            }
        }
        SNAPPY = attempt;
    }

    public static boolean available()
    {
        return SNAPPY != null;
    }

    public static Compression instance()
    {
        return SNAPPY;
    }

    private static int maxUncompressedLength(ByteBuffer compressed)
    {
        return VariableLengthQuantity.readVariableLengthInt(ByteBuffers.duplicate(compressed));
    }
}
