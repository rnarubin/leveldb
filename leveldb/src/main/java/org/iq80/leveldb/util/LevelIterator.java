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

import org.iq80.leveldb.impl.FileMetaData;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.InternalKeyComparator;
import org.iq80.leveldb.impl.ReverseSeekingIterator;
import org.iq80.leveldb.impl.TableCache;
import org.iq80.leveldb.util.LevelIterator.FileIterator;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import org.iq80.leveldb.table.TableIterator;

import com.google.common.collect.Maps;

public final class LevelIterator
        extends TwoStageIterator<FileIterator, TableIterator, FileMetaData>
{
    private final TableCache tableCache;

    public LevelIterator(
            TableCache tableCache,
            List<FileMetaData> files,
            InternalKeyComparator comparator)
    {
        super(new FileIterator(files, comparator));
        this.tableCache = tableCache;
    }

    protected TableIterator getData(FileMetaData file)
    {
        return tableCache.newIterator(file);
    }

    static final class FileIterator
            implements ReverseSeekingIterator<InternalKey, FileMetaData>, Closeable
    {
        private final List<FileMetaData> files;
        private final InternalKeyComparator comparator;
        private int index;

        public FileIterator(List<FileMetaData> files, InternalKeyComparator comparator)
        {
            this.files = files;
            this.comparator = comparator;
        }

        @Override
        public void seekToFirst()
        {
            index = 0;
        }

        @Override
        public void seek(InternalKey targetKey)
        {
            // seek the index to the block containing the key
            if (files.size() == 0) {
                return;
            }

            // todo replace with Collections.binarySearch
            int left = 0;
            int right = files.size() - 1;

            // binary search restart positions to find the restart position immediately before the targetKey
            while (left < right) {
                int mid = (left + right) / 2;

                if (comparator.compare(files.get(mid).getLargest(), targetKey) < 0) {
                    // Key at "mid.largest" is < "target". Therefore all
                    // files at or before "mid" are uninteresting.
                    left = mid + 1;
                }
                else {
                    // Key at "mid.largest" is >= "target". Therefore all files
                    // after "mid" are uninteresting.
                    right = mid;
                }
            }
            index = right;

            // if the index is now pointing to the last block in the file, check if the largest key in
            // the block is less than the target key. If so, we need to seek beyond the end of this file
            if (index == files.size() - 1 && comparator.compare(files.get(index).getLargest(), targetKey) < 0) {
                index++;
            }
        }

        @Override
        public Entry<InternalKey, FileMetaData> peek()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Entry<InternalKey, FileMetaData> next()
        {
            FileMetaData f = files.get(index++);
            return Maps.immutableEntry(f.getLargest(), f);
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext()
        {
            return index < files.size();
        }

        @Override
        public Entry<InternalKey, FileMetaData> peekPrev()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Entry<InternalKey, FileMetaData> prev()
        {
            FileMetaData f = files.get(--index);
            return Maps.immutableEntry(f.getLargest(), f);
        }

        @Override
        public boolean hasPrev()
        {
            return index > 0;
        }

        @Override
        public void close()
                throws IOException
        {
            // noop
        }

        @Override
        public void seekToLast()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void seekToEnd()
        {
            index = files.size();
        }

    }
}