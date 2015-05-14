package org.iq80.leveldb.impl;

import org.iq80.leveldb.Options;
import org.iq80.leveldb.Options.IOImpl;
import org.iq80.leveldb.util.Slice;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.nio.channels.FileChannel;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public abstract class TestLogWriter
{
    private Options options;

    protected TestLogWriter(Options options)
    {
        this.options = options;
    }

    @Test
    public void testLogRecordBounds()
            throws Exception
    {
        File file = File.createTempFile("test", ".log");
        try {
            int recordSize = LogConstants.BLOCK_SIZE - LogConstants.HEADER_SIZE;
            Slice record = new Slice(recordSize);

            LogWriter writer = Logs.createLogWriter(file, 10, options);
            writer.addRecord(record, true);
            writer.close();

            LogMonitor logMonitor = new AssertNoCorruptionLogMonitor();

            try (@SuppressWarnings("resource")
            FileChannel channel = new FileInputStream(file).getChannel()) {

                LogReader logReader = new LogReader(channel, logMonitor, true, 0);

                int count = 0;
                for (Slice slice = logReader.readRecord(); slice != null; slice = logReader.readRecord()) {
                    assertEquals(slice.length(), recordSize);
                    count++;
                }
                assertEquals(count, 1);
            }
        }
        finally {
            file.delete();
        }
    }

    private static class AssertNoCorruptionLogMonitor
            implements LogMonitor
    {

        @Override
        public void corruption(long bytes, String reason)
        {
            fail("corruption at " + bytes + " reason: " + reason);
        }

        @Override
        public void corruption(long bytes, Throwable reason)
        {
            fail("corruption at " + bytes + " reason: " + reason.toString());
        }
    }

    public static class TestFileChannel
            extends TestLogWriter
    {
        public TestFileChannel()
        {
            super(new Options().ioImplementation(IOImpl.FILE));
        }
    }

    public static class TestMMap
            extends TestLogWriter
    {
        public TestMMap()
        {
            super(new Options().ioImplementation(IOImpl.MMAP));
        }
    }
}
