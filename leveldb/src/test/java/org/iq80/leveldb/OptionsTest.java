package org.iq80.leveldb;

import java.lang.reflect.Field;
import java.util.Properties;

import org.iq80.leveldb.Options.IOImpl;
import org.testng.Assert;
import org.testng.annotations.Test;

public abstract class OptionsTest
{
    @Test
    public void testPropertiesConfig()
    {
        Options o;
        Properties props = new Properties(System.getProperties());
        props.setProperty("leveldb.options.createIfMissing", "false");
        props.setProperty("leveldb.options.errorIfExists", "true");
        props.setProperty("leveldb.options.writeBufferSize", "123");
        props.setProperty("leveldb.options.maxOpenFiles", "456");
        props.setProperty("leveldb.options.compressionType", "NONE");
        props.setProperty("leveldb.options.ioImplementation", "FILE");
        Options.readProperties(props);
        o = getOptions();
        Assert.assertEquals(o.createIfMissing(), false);
        Assert.assertEquals(o.errorIfExists(), true);
        Assert.assertEquals(o.writeBufferSize(), 123);
        Assert.assertEquals(o.maxOpenFiles(), 456);
        Assert.assertEquals(o.compressionType(), CompressionType.NONE);
        Assert.assertEquals(o.ioImplemenation(), IOImpl.FILE);

        props.setProperty("leveldb.options.createIfMissing", "true");
        props.setProperty("leveldb.options.errorIfExists", "false");
        props.setProperty("leveldb.options.writeBufferSize", "" + (4 << 20));
        props.setProperty("leveldb.options.maxOpenFiles", "1000");
        props.setProperty("leveldb.options.compressionType", "SNAPPY");
        props.setProperty("leveldb.options.ioImplementation", Options.USE_MMAP_DEFAULT ? "MMAP" : "FILE");

        Options.readProperties(props);
        o = getOptions();
        Assert.assertEquals(o.createIfMissing(), true);
        Assert.assertEquals(o.errorIfExists(), false);
        Assert.assertEquals(o.writeBufferSize(), 4 << 20);
        Assert.assertEquals(o.maxOpenFiles(), 1000);
        Assert.assertEquals(o.compressionType(), CompressionType.SNAPPY);
        Assert.assertEquals(o.ioImplemenation(), Options.USE_MMAP_DEFAULT ? IOImpl.MMAP : IOImpl.FILE);
    }

    @Test
    public void testCopy()
    {
        Options a = getOptions();
        Options b = Options.copy(a);
        assertReflectionEquals(b, a, Options.class);

        a.blockRestartInterval(789).errorIfExists(true).comparator(null).compressionType(CompressionType.NONE);
        try {
            assertReflectionEquals(b, a, Options.class);
        }
        catch (AssertionError expected) {
        }

        Options c = Options.copy(a);
        assertReflectionEquals(c, a, Options.class);
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

    public static class LegacyOptionsTest
            extends OptionsTest
    {
        @SuppressWarnings("deprecation")
        @Override
        Options getOptions()
        {
            return new Options();
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
    }
}
