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
import java.nio.ByteOrder;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class VariableLengthQuantityTest
{
    @Test
    public void testWriteVariableLengthInt()
    {
        testVariableLengthInt(0x0);
        testVariableLengthInt(0xf);
        testVariableLengthInt(0xff);
        testVariableLengthInt(0xfff);
        testVariableLengthInt(0xffff);
        testVariableLengthInt(0xfffff);
        testVariableLengthInt(0xffffff);
        testVariableLengthInt(0xfffffff);
        testVariableLengthInt(0xffffffff);
    }

    private static void testVariableLengthInt(int value)
    {
        ByteBuffer output = ByteBuffer.allocate(5).order(ByteOrder.LITTLE_ENDIAN);
        VariableLengthQuantity.writeVariableLengthInt(value, output);
        assertEquals(output.position(), VariableLengthQuantity.variableLengthSize(value));
        output.flip();
        int actual = VariableLengthQuantity.readVariableLengthInt(output);
        assertEquals(actual, value);
    }

    @Test
    public void testWriteVariableLengthLong()
    {
        testVariableLengthLong(0x0L);
        testVariableLengthLong(0xfL);
        testVariableLengthLong(0xffL);
        testVariableLengthLong(0xfffL);
        testVariableLengthLong(0xffffL);
        testVariableLengthLong(0xfffffL);
        testVariableLengthLong(0xffffffL);
        testVariableLengthLong(0xfffffffL);
        testVariableLengthLong(0xffffffffL);
        testVariableLengthLong(0xfffffffffL);
        testVariableLengthLong(0xffffffffffL);
        testVariableLengthLong(0xfffffffffffL);
        testVariableLengthLong(0xffffffffffffL);
        testVariableLengthLong(0xfffffffffffffL);
        testVariableLengthLong(0xffffffffffffffL);
        testVariableLengthLong(0xfffffffffffffffL);
        testVariableLengthLong(0xffffffffffffffffL);
    }

    private static void testVariableLengthLong(long value)
    {
        ByteBuffer output = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
        VariableLengthQuantity.writeVariableLengthLong(value, output);
        assertEquals(output.position(), VariableLengthQuantity.variableLengthSize(value));
        output.flip();
        long actual = VariableLengthQuantity.readVariableLengthLong(output);
        assertEquals(actual, value);
    }
}
