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

package org.iq80.leveldb;

import java.lang.reflect.Field;
import java.util.Properties;

import org.iq80.leveldb.util.MemoryManagers;
import org.testng.Assert;
import org.testng.annotations.Test;

public abstract class OptionsTest
{
    @Test
    public void testPropertiesConfig()
    {
        Options o;
        ReadOptions r;
        WriteOptions w;
        Properties props = new Properties(System.getProperties());
        props.setProperty("leveldb.Options.createIfMissing", "false");
        props.setProperty("leveldb.Options.errorIfExists", "true");
        props.setProperty("leveldb.Options.writeBufferSize", "123");
        props.setProperty("leveldb.Options.maxOpenFiles", "456");
        props.setProperty("leveldb.Options.memoryManager", MemoryManagers.class.getName() + ".heap");
        props.setProperty("leveldb.ReadOptions.verifyChecksums", "true");
        props.setProperty("leveldb.WriteOptions.sync", "true");
        reloadOptions(props, "leveldb.Options.", Options.class, "DEFAULT_OPTIONS");
        reloadOptions(props, "leveldb.ReadOptions.", ReadOptions.class, "DEFAULT_READ_OPTIONS");
        reloadOptions(props, "leveldb.WriteOptions.", WriteOptions.class, "DEFAULT_WRITE_OPTIONS");
        o = getOptions();
        r = getReadOptions();
        w = getWriteOptions();
        Assert.assertEquals(o.createIfMissing(), false);
        Assert.assertEquals(o.errorIfExists(), true);
        Assert.assertEquals(o.writeBufferSize(), 123);
        Assert.assertEquals(o.maxOpenFiles(), 456);
        Assert.assertEquals(o.memoryManager(), MemoryManagers.heap()); // depends on singleton property of heap instance
        Assert.assertEquals(r.verifyChecksums(), true);
        Assert.assertEquals(w.sync(), true);

        props.setProperty("leveldb.Options.createIfMissing", "true");
        props.setProperty("leveldb.Options.errorIfExists", "false");
        props.setProperty("leveldb.Options.writeBufferSize", "" + (4 << 20));
        props.setProperty("leveldb.Options.maxOpenFiles", "1000");
        props.setProperty("leveldb.Options.memoryManager", "null");
        props.setProperty("leveldb.ReadOptions.verifyChecksums", "false");
        props.setProperty("leveldb.ReadOptions.verifyChecksums", "false");
        props.setProperty("leveldb.WriteOptions.sync", "false");

        reloadOptions(props, "leveldb.Options.", Options.class, "DEFAULT_OPTIONS");
        reloadOptions(props, "leveldb.ReadOptions.", ReadOptions.class, "DEFAULT_READ_OPTIONS");
        reloadOptions(props, "leveldb.WriteOptions.", WriteOptions.class, "DEFAULT_WRITE_OPTIONS");
        o = getOptions();
        r = getReadOptions();
        w = getWriteOptions();
        Assert.assertEquals(o.createIfMissing(), true);
        Assert.assertEquals(o.errorIfExists(), false);
        Assert.assertEquals(o.writeBufferSize(), 4 << 20);
        Assert.assertEquals(o.maxOpenFiles(), 1000);
        Assert.assertEquals(o.memoryManager(), null);
        Assert.assertEquals(r.verifyChecksums(), false);
        Assert.assertEquals(w.sync(), false);
    }

    @SuppressWarnings("unchecked")
    private static <T> void reloadOptions(Properties properties, String prefix, Class<T> clazz, String defaultName)
    {
        try {
            Field defaultOptions = clazz.getDeclaredField(defaultName);
            defaultOptions.setAccessible(true);
            OptionsConfiguration.populateFromProperties(properties, prefix, clazz, (T) defaultOptions.get(null));
        }
        catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
            throw new Error(e);
        }
    }

    @Test
    public void testCopy()
    {
        {
            Options a = getOptions();
            Options b = Options.copy(a);
            assertReflectionEquals(b, a, Options.class);

            a.blockRestartInterval(789).errorIfExists(true).compression(null);
            try {
                assertReflectionEquals(b, a, Options.class);
            }
            catch (AssertionError expected) {
            }

            Options c = Options.copy(a);
            assertReflectionEquals(c, a, Options.class);
        }
        {
            ReadOptions a = getReadOptions();
            ReadOptions b = ReadOptions.copy(a);
            assertReflectionEquals(b, a, ReadOptions.class);

            a.fillCache(false).verifyChecksums(true);
            try {
                assertReflectionEquals(b, a, ReadOptions.class);
            }
            catch (AssertionError expected) {
            }

            ReadOptions c = ReadOptions.copy(a);
            assertReflectionEquals(c, a, ReadOptions.class);
        }
        {
            WriteOptions a = getWriteOptions();
            WriteOptions b = WriteOptions.copy(a);
            assertReflectionEquals(b, a, WriteOptions.class);

            a.snapshot(true).sync(true);
            try {
                assertReflectionEquals(b, a, WriteOptions.class);
            }
            catch (AssertionError expected) {
            }

            WriteOptions c = WriteOptions.copy(a);
            assertReflectionEquals(c, a, WriteOptions.class);
        }
    }

    private <T> void assertReflectionEquals(T actual, T expected, Class<T> c)
    {
        for (Field f : c.getDeclaredFields()) {
            if (!f.getDeclaringClass().equals(c))
                continue;

            f.setAccessible(true);
            try {
                Assert.assertEquals(f.get(actual), f.get(expected), "mismatch on " + f.getName());
            }
            catch (IllegalArgumentException | IllegalAccessException e) {
                throw new Error(e);
            }
        }
    }

    abstract Options getOptions();

    abstract ReadOptions getReadOptions();

    abstract WriteOptions getWriteOptions();

    public static class LegacyOptionsTest
            extends OptionsTest
    {
        @SuppressWarnings("deprecation")
        @Override
        Options getOptions()
        {
            return new Options();
        }

        @SuppressWarnings("deprecation")
        @Override
        ReadOptions getReadOptions()
        {
            return new ReadOptions();
        }

        @SuppressWarnings("deprecation")
        @Override
        WriteOptions getWriteOptions()
        {
            return new WriteOptions();
        }
    }

    public static class ContemporaryOptionsTest
            extends OptionsTest
    {
        @Override
        Options getOptions()
        {
            return Options.make();
        }

        @Override
        ReadOptions getReadOptions()
        {
            return ReadOptions.make();
        }

        @Override
        WriteOptions getWriteOptions()
        {
            return WriteOptions.make();
        }
    }
}
