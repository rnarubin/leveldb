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

import java.nio.ByteBuffer;

public final class VariableLengthQuantity
{
    private VariableLengthQuantity()
    {
    }

    public static int variableLengthSize(int value)
    {
        int size = 1;
        while ((value & (~0x7f)) != 0) {
            value >>>= 7;
            size++;
        }
        return size;
    }

    public static int variableLengthSize(long value)
    {
        int size = 1;
        while ((value & (~0x7fL)) != 0) {
            value >>>= 7;
            size++;
        }
        return size;
    }

    public static void writeVariableLengthInt(int value, ByteBuffer dst)
    {
        int highBitMask = 0x80;
        if (value < (1 << 7) && value >= 0) {
            dst.put((byte) value);
        }
        else if (value < (1 << 14) && value > 0) {
            dst.put((byte) (value | highBitMask));
            dst.put((byte) (value >>> 7));
        }
        else if (value < (1 << 21) && value > 0) {
            dst.put((byte) (value | highBitMask));
            dst.put((byte) ((value >>> 7) | highBitMask));
            dst.put((byte) (value >>> 14));
        }
        else if (value < (1 << 28) && value > 0) {
            dst.put((byte) (value | highBitMask));
            dst.put((byte) ((value >>> 7) | highBitMask));
            dst.put((byte) ((value >>> 14) | highBitMask));
            dst.put((byte) (value >>> 21));
        }
        else {
            dst.put((byte) (value | highBitMask));
            dst.put((byte) ((value >>> 7) | highBitMask));
            dst.put((byte) ((value >>> 14) | highBitMask));
            dst.put((byte) ((value >>> 21) | highBitMask));
            dst.put((byte) (value >>> 28));
        }
    }

    public static void writeVariableLengthInt(int value, GrowingBuffer buffer)
    {
        writeVariableLengthInt(value, buffer.ensureSpace(5));
    }

    public static void writeVariableLengthLong(long value, ByteBuffer buffer)
    {
        // while value more than the first 7 bits set
        while ((value & (~0x7fL)) != 0) {
            buffer.put((byte) ((value & 0x7f) | 0x80));
            value >>>= 7;
        }
        buffer.put((byte) value);
    }

    public static void writeVariableLengthLong(long value, GrowingBuffer buffer)
    {
        writeVariableLengthLong(value, buffer.ensureSpace(10));
    }

    public static int readVariableLengthInt(ByteBuffer sliceInput)
    {
        int result = 0;
        for (int shift = 0; shift <= 28; shift += 7) {
            int b = ByteBuffers.readUnsignedByte(sliceInput);

            // add the lower 7 bits to the result
            result |= ((b & 0x7f) << shift);

            // if high bit is not set, this is the last byte in the number
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new NumberFormatException("last byte of variable length int has high bit set");
    }

    public static long readVariableLengthLong(ByteBuffer sliceInput)
    {
        long result = 0;
        for (int shift = 0; shift <= 63; shift += 7) {
            long b = ByteBuffers.readUnsignedByte(sliceInput);

            // add the lower 7 bits to the result
            result |= ((b & 0x7f) << shift);

            // if high bit is not set, this is the last byte in the number
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new NumberFormatException("last byte of variable length int has high bit set");
    }
}
