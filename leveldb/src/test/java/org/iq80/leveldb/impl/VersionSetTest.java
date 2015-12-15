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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;

import org.iq80.leveldb.Env.DBHandle;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.table.BytewiseComparator;
import org.iq80.leveldb.util.InternalIterator;
import org.iq80.leveldb.util.MemoryManagers;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.util.concurrent.SettableFuture;

public class VersionSetTest
{

    @Test
    public void testWeakReferenceGet()
            throws IOException, InterruptedException, ExecutionException
    {
        final InternalKeyComparator cmp = new InternalKeyComparator(new BytewiseComparator());
        final Options options = Options.make()
                .env(new FileChannelEnv(MemoryManagers.heap()))
                .memoryManager(MemoryManagers.heap());
        final ByteBuffer userKey = ByteBuffer.wrap(new byte[] { 0 });
        final InternalKey key = new TransientInternalKey(userKey, 0, ValueType.VALUE);
        final FileMetaData file = new FileMetaData(1, 10, key, key);
        final DBHandle db = FileSystemEnv.handle(Files.createTempDirectory("leveldb-testing"));
        try {

            final Semaphore blocker = new Semaphore(0);
            final TableCache tableCache = new TableCache(db, 10, cmp, options, null)
            {
                @Override
                public InternalIterator newIterator(final FileMetaData file)
                {
                    blocker.acquireUninterruptibly();
                    return new EmptyIterator();
                }
            };

            try (VersionSet vs = new VersionSet(db, tableCache, cmp, options)) {
                {
                    final VersionEdit edit = new VersionEdit();
                    edit.addFile(1, file);
                    vs.logAndApply(edit);
                }
                Assert.assertFalse(vs.getLiveFiles().isEmpty());
                Assert.assertEquals(vs.getLiveFiles().iterator().next().getNumber(), file.getNumber());

                final SettableFuture<LookupResult> lookup = SettableFuture.create();
                // lookup will block until semaphore released
                new Thread(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try {
                            lookup.set(vs.get(new LookupKey(ByteBuffer.wrap(new byte[] { 0 }), 0)));
                        }
                        catch (final IOException e) {
                            lookup.setException(e);
                        }
                    }
                }).start();

                {
                    final VersionEdit edit = new VersionEdit();
                    edit.deleteFile(1, file.getNumber());
                    vs.logAndApply(edit);
                }

                System.gc();
                Assert.assertFalse(vs.getLiveFiles().isEmpty());
                Assert.assertEquals(vs.getLiveFiles().iterator().next().getNumber(), file.getNumber());
                blocker.release();
                lookup.get();
                System.gc();
                Assert.assertTrue(vs.getLiveFiles().isEmpty());
            }
        }
        finally {
            options.env().deleteDir(db);
        }
    }

    private static class EmptyIterator
            implements InternalIterator
    {

        @Override
        public void seekToEnd()
        {
        }

        @Override
        public void seekToFirst()
        {
        }

        @Override
        public void seek(InternalKey targetKey)
        {
        }

        @Override
        public Entry<InternalKey, ByteBuffer> peek()
        {
            throw new NoSuchElementException();
        }

        @Override
        public Entry<InternalKey, ByteBuffer> next()
        {
            throw new NoSuchElementException();
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext()
        {
            return false;
        }

        @Override
        public Entry<InternalKey, ByteBuffer> peekPrev()
        {
            throw new NoSuchElementException();
        }

        @Override
        public Entry<InternalKey, ByteBuffer> prev()
        {
            throw new NoSuchElementException();
        }

        @Override
        public boolean hasPrev()
        {
            return false;
        }

        @Override
        public void close()
                throws IOException
        {
        }

    }

}


