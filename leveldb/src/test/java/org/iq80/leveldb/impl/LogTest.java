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
package org.iq80.leveldb.impl;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multiset;

import org.iq80.leveldb.Env;
import org.iq80.leveldb.FileInfo;
import org.iq80.leveldb.Env.SequentialReadFile;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.DbImplTest.StrictEnv;
import org.iq80.leveldb.impl.DbImplTest.StrictMemoryManager;
import org.iq80.leveldb.util.ByteBuffers;
import org.iq80.leveldb.util.ConcurrencyHelper;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Charsets.UTF_8;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.FileAssert.fail;

public abstract class LogTest
{

    protected abstract Options getOptions();

    protected abstract Env getEnv();

    private static final LogMonitor NO_CORRUPTION_MONITOR = new LogMonitor()
    {
        @Override
        public void corruption(long bytes, String reason)
        {
            fail(String.format("corruption of %s bytes: %s", bytes, reason));
        }

        @Override
        public void corruption(long bytes, Throwable reason)
        {
            throw new RuntimeException(String.format("corruption of %s bytes: %s", bytes, reason), reason);
        }
    };

    private LogWriter writer;
    private FileInfo filePath;

    @Test
    public void testEmptyBlock()
            throws Exception
    {
        testLog();
    }

    @Test
    public void testSmallRecord()
            throws Exception
    {
        testLog(toByteBuffer("dain sundstrom"));
    }

    @Test
    public void testMultipleSmallRecords()
            throws Exception
    {
        List<ByteBuffer> records = asList(
                toByteBuffer("Lagunitas  Little Sumpin’ Sumpin’"),
                toByteBuffer("Lagunitas IPA"),
                toByteBuffer("Lagunitas Imperial Stout"),
                toByteBuffer("Oban 14"),
                toByteBuffer("Highland Park"),
                toByteBuffer("Lagavulin"));

        testLog(records);

        testConcurrentLog(records, true, 3);
    }

    @Test
    public void testLargeRecord()
            throws Exception
    {
        testLog(toByteBuffer("dain sundstrom", 4000));
    }

    @Test
    public void testMultipleLargeRecords()
            throws Exception
    {
        List<ByteBuffer> records = asList(
                toByteBuffer("Lagunitas  Little Sumpin’ Sumpin’", 4000),
                toByteBuffer("Lagunitas IPA", 4000),
                toByteBuffer("Lagunitas Imperial Stout", 4000),
                toByteBuffer("Oban 14", 4000),
                toByteBuffer("Highland Park", 4000),
                toByteBuffer("Lagavulin", 4000));

        testLog(records);

        testConcurrentLog(records, true, 3);
    }

    @Test
    public void testManySmallRecordsConcurrently()
            throws InterruptedException, ExecutionException, IOException
    {
        Random rand = new Random(0);
        List<ByteBuffer> records = new ArrayList<>();
        for (int i = 0; i < 1_000_000; i++) {
            byte[] b = new byte[rand.nextInt(20) + 5];
            rand.nextBytes(b);
            records.add(toByteBuffer(new String(b, StandardCharsets.UTF_8)));
        }

        testConcurrentLog(records, true, 8);
    }

    @Test
    public void testManyLargeRecordsConcurrently()
            throws InterruptedException, ExecutionException, IOException
    {
        Random rand = new Random(0);
        List<ByteBuffer> records = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            byte[] b = new byte[rand.nextInt(20) + 5];
            rand.nextBytes(b);
            records.add(toByteBuffer(new String(b, StandardCharsets.UTF_8), rand.nextInt(2000) + 2000));
        }

        testConcurrentLog(records, true, 8);
    }

    @Test
    public void testManyHugeRecordsConcurrently()
            throws InterruptedException, ExecutionException, IOException
    {
        //larger than page size to test mmap edges
        Random rand = new Random(0);
        List<ByteBuffer> records = new ArrayList<>();
        ByteBuffer buf = ByteBuffer.allocate(2 * 1024 * 1024);
        while (buf.hasRemaining()) {
            byte[] b = new byte[1024];
            rand.nextBytes(b);
            buf.put(b);
        }
        buf.flip();
        for (int i = 0; i < 1000; i++) {
            records.add(ByteBuffers.duplicateByLength(buf, 0, rand.nextInt(1_000_000) + 1_000_000));
        }

        testConcurrentLog(records, true, 8);
    }

    @Test
    public void testReadWithoutProperClose()
            throws Exception
    {
        testLog(ImmutableList.of(toByteBuffer("something"), toByteBuffer("something else")), false);
    }

    private void testLog(ByteBuffer... entries)
            throws IOException
    {
        testLog(asList(entries));
    }

    private void testLog(List<ByteBuffer> records)
            throws IOException
    {
        testLog(records, true);
    }

    private void testLog(List<ByteBuffer> records, boolean closeWriter)
            throws IOException
    {
        for (ByteBuffer entry : records) {
            writer.addRecord(ByteBuffers.duplicate(entry), true);
        }

        if (closeWriter) {
            writer.close();
        }

        try (SequentialReadFile fileInput = getEnv().openSequentialReadFile(filePath);
                StrictMemoryManager strictMemory = new StrictMemoryManager();
                LogReader reader = new LogReader(fileInput, NO_CORRUPTION_MONITOR, true, 0, strictMemory)) {

            for (ByteBuffer expected : records) {
                ByteBuffer actual = reader.readRecord();
                assertEquals(actual, expected);
                strictMemory.free(actual);
            }
            assertNull(reader.readRecord());
        }
    }

    private void testConcurrentLog(List<ByteBuffer> record, boolean closeWriter, int threads)
            throws InterruptedException, ExecutionException, IOException
    {
        Multiset<ByteBuffer> recordBag = HashMultiset.create();
        List<Callable<Void>> work = new ArrayList<>(record.size());
        for (final ByteBuffer s : record) {
            work.add(new Callable<Void>()
            {
                @Override
                public Void call()
                        throws IOException
                {
                    writer.addRecord(ByteBuffers.duplicate(s), false);
                    return null;
                }
            });
            recordBag.add(s);
        }
        try (ConcurrencyHelper<Void> c = new ConcurrencyHelper<Void>(threads, "testConcurrentLog")) {
            c.submitAllAndWaitIgnoringResults(work);
        }

        if (closeWriter) {
            writer.close();
        }

        try (StrictMemoryManager strictMemory = new StrictMemoryManager();
                SequentialReadFile fileInput = getEnv().openSequentialReadFile(filePath);
                LogReader reader = new LogReader(fileInput, NO_CORRUPTION_MONITOR, true, 0, strictMemory)) {

            for (ByteBuffer actual = reader.readRecord(); actual != null; actual = reader.readRecord()) {
                Assert.assertTrue(recordBag.remove(actual), "Found slice in log that was not added");
                strictMemory.free(actual);
            }
            Assert.assertEquals(recordBag.size(), 0, "Not all added slices found in log");
        }
    }

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        filePath = FileInfo.log(FileSystemEnv.handle(Files.createTempDirectory("leveldb")), 42);
        writer = Logs.createLogWriter(filePath, 42, getOptions());
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        writer.close();
        if (filePath != null && getEnv().fileExists(filePath)) {
            getEnv().deleteFile(filePath);
        }
    }

    @Test
    public void testLogRecordBounds()
            throws Exception
    {
        try {
            int recordSize = LogConstants.BLOCK_SIZE - LogConstants.HEADER_SIZE;
            ByteBuffer record = ByteBuffer.allocate(recordSize);

            LogWriter writer = Logs.createLogWriter(filePath, 10, getOptions());
            writer.addRecord(record, true);
            writer.close();

            LogMonitor logMonitor = new AssertNoCorruptionLogMonitor();

            try (StrictMemoryManager strictMemory = new StrictMemoryManager();
                    SequentialReadFile fileInput = getEnv().openSequentialReadFile(filePath);
                    LogReader logReader = new LogReader(fileInput, logMonitor, true, 0, strictMemory)) {

                int count = 0;
                for (ByteBuffer slice = logReader.readRecord(); slice != null; slice = logReader.readRecord()) {
                    assertEquals(slice.remaining(), recordSize);
                    count++;
                    strictMemory.free(slice);
                }
                assertEquals(count, 1);
            }
        }
        finally {
            getEnv().deleteFile(filePath);
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

    static ByteBuffer toByteBuffer(String value)
    {
        return toByteBuffer(value, 1);
    }

    static ByteBuffer toByteBuffer(String value, int times)
    {
        byte[] bytes = value.getBytes(UTF_8);
        ByteBuffer slice = ByteBuffer.allocate(bytes.length * times);
        for (int i = 0; i < times; i++) {
            slice.put(bytes);
        }
        slice.flip();
        return slice;
    }

    public static class FileLogTest
            extends LogTest
    {
        private StrictEnv env;
        private Options options;

        protected Options getOptions()
        {
            return options;
        }

        protected Env getEnv()
        {
            return env;
        }

        @BeforeClass
        public void open()
        {
            env = new StrictEnv();
            options = Options.make().env(env).memoryManager(env.strictMemory);
        }

        @AfterClass
        public void closeMemory()
                throws IOException
        {
            env.close();
        }
    }

    // TODO mmap env
    // public static class MMapLogTest
    // extends LogTest
    // {
    // }
}
