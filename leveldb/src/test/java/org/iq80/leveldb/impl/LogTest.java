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

import org.iq80.leveldb.Options;
import org.iq80.leveldb.Options.IOImpl;
import org.iq80.leveldb.util.Closeables;
import org.iq80.leveldb.util.ConcurrencyHelper;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.SliceOutput;
import org.iq80.leveldb.util.Slices;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
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

    protected LogTest(Options options)
    {
        this.options = options;
    }

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
    private Options options;

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
        testLog(toSlice("dain sundstrom"));
    }

    @Test
    public void testMultipleSmallRecords()
            throws Exception
    {
        List<Slice> records = asList(
                toSlice("Lagunitas  Little Sumpin’ Sumpin’"),
                toSlice("Lagunitas IPA"),
                toSlice("Lagunitas Imperial Stout"),
                toSlice("Oban 14"),
                toSlice("Highland Park"),
                toSlice("Lagavulin"));

        testLog(records);

        testConcurrentLog(records, true, 3);
    }

    @Test
    public void testLargeRecord()
            throws Exception
    {
        testLog(toSlice("dain sundstrom", 4000));
    }

    @Test
    public void testMultipleLargeRecords()
            throws Exception
    {
        List<Slice> records = asList(
                toSlice("Lagunitas  Little Sumpin’ Sumpin’", 4000),
                toSlice("Lagunitas IPA", 4000),
                toSlice("Lagunitas Imperial Stout", 4000),
                toSlice("Oban 14", 4000),
                toSlice("Highland Park", 4000),
                toSlice("Lagavulin", 4000));

        testLog(records);

        testConcurrentLog(records, true, 3);
    }

    @Test
    public void testManySmallRecordsConcurrently()
            throws InterruptedException, ExecutionException, IOException
    {
        Random rand = new Random(0);
        List<Slice> records = new ArrayList<>();
        for (int i = 0; i < 1_000_000; i++) {
            byte[] b = new byte[rand.nextInt(20) + 5];
            rand.nextBytes(b);
            records.add(toSlice(new String(b, StandardCharsets.UTF_8)));
        }

        testConcurrentLog(records, true, 8);
    }

    @Test
    public void testManyLargeRecordsConcurrently()
            throws InterruptedException, ExecutionException, IOException
    {
        Random rand = new Random(0);
        List<Slice> records = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            byte[] b = new byte[rand.nextInt(20) + 5];
            rand.nextBytes(b);
            records.add(toSlice(new String(b, StandardCharsets.UTF_8), 4000));
        }

        testConcurrentLog(records, true, 8);
    }

    @Test
    public void testManyHugeRecordsConcurrently()
            throws InterruptedException, ExecutionException, IOException
    {
        //larger than page size to test mmap edges
        Random rand = new Random(0);
        List<Slice> records = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            byte[] b = new byte[rand.nextInt(20) + 5];
            rand.nextBytes(b);
            records.add(toSlice(new String(b, StandardCharsets.UTF_8), 200000));
        }

        testConcurrentLog(records, true, 8);
    }

    @Test
    public void testReadWithoutProperClose()
            throws Exception
    {
        testLog(ImmutableList.of(toSlice("something"), toSlice("something else")), false);
    }

    private void testLog(Slice... entries)
            throws IOException
    {
        testLog(asList(entries));
    }

    private void testLog(List<Slice> records)
            throws IOException
    {
        testLog(records, true);
    }

    private void testLog(List<Slice> records, boolean closeWriter)
            throws IOException
    {
        for (Slice entry : records) {
            writer.addRecord(entry, true);
        }

        if (closeWriter) {
            writer.close();
        }

        // test readRecord
        @SuppressWarnings("resource")
        FileChannel fileChannel = new FileInputStream(writer.getFile()).getChannel();
        try {
            LogReader reader = new LogReader(fileChannel, NO_CORRUPTION_MONITOR, true, 0);
            for (Slice expected : records) {
                Slice actual = reader.readRecord();
                assertEquals(actual, expected);
            }
            assertNull(reader.readRecord());
        }
        finally {
            Closeables.closeQuietly(fileChannel);
        }
    }

    @SuppressWarnings("resource")
    private void testConcurrentLog(List<Slice> record, boolean closeWriter, int threads)
            throws InterruptedException, ExecutionException, IOException
    {
        Multiset<Slice> recordBag = HashMultiset.create();
        List<Callable<Void>> work = new ArrayList<>(record.size());
        for (final Slice s : record) {
            work.add(new Callable<Void>()
            {
                @Override
                public Void call()
                        throws IOException
                {
                    writer.addRecord(s, false);
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

        try (FileChannel fileChannel = new FileInputStream(writer.getFile()).getChannel()) {
            LogReader reader = new LogReader(fileChannel, NO_CORRUPTION_MONITOR, true, 0);
            for (Slice actual = reader.readRecord(); actual != null; actual = reader.readRecord()) {
                Assert.assertTrue(recordBag.remove(actual), "Found slice in log that was not added");
            }
            Assert.assertEquals(recordBag.size(), 0, "Not all added slices found in log");
        }
    }

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        writer = Logs.createLogWriter(File.createTempFile("table", ".log"), 42, options);
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        if (writer != null) {
            writer.delete();
        }
    }

    static Slice toSlice(String value)
    {
        return toSlice(value, 1);
    }

    static Slice toSlice(String value, int times)
    {
        byte[] bytes = value.getBytes(UTF_8);
        Slice slice = Slices.allocate(bytes.length * times);
        SliceOutput sliceOutput = slice.output();
        for (int i = 0; i < times; i++) {
            sliceOutput.writeBytes(bytes);
        }
        return slice;
    }

    public static class FileLogTest
            extends LogTest
    {
        public FileLogTest()
        {
            super(new Options().ioImplementation(IOImpl.FILE));
        }
    }

    public static class MMapLogTest
            extends LogTest
    {
        public MMapLogTest()
        {
            super(new Options().ioImplementation(IOImpl.MMAP));
        }
    }
}
