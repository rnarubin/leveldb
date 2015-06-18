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

import org.iq80.leveldb.Options;
import org.iq80.leveldb.Options.IOImpl;
import org.iq80.leveldb.impl.DbImplTest.StrictMemoryManager;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;

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
            ByteBuffer record = ByteBuffer.allocate(recordSize);

            LogWriter writer = Logs.createLogWriter(file, 10, options);
            writer.addRecord(record, true);
            writer.close();

            LogMonitor logMonitor = new AssertNoCorruptionLogMonitor();

            try (StrictMemoryManager strictMemory = new StrictMemoryManager();
                    FileInputStream fileInput = new FileInputStream(file);
                    LogReader logReader = new LogReader(fileInput.getChannel(), logMonitor, true, 0, strictMemory)) {

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
            super(Options.make().ioImplementation(IOImpl.FILE));
        }
    }

    public static class TestMMap
            extends TestLogWriter
    {
        public TestMMap()
        {
            super(Options.make().ioImplementation(IOImpl.MMAP));
        }
    }
}
