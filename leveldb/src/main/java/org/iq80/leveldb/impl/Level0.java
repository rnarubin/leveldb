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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Lists;

import org.iq80.leveldb.DBBufferComparator;
import org.iq80.leveldb.impl.MemTable.MemTableIterator;
import org.iq80.leveldb.table.TableIterator;
import org.iq80.leveldb.util.DbIterator;
import org.iq80.leveldb.util.InternalIterator;
import org.iq80.leveldb.util.LevelIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;

import static com.google.common.collect.Lists.newArrayList;
import static org.iq80.leveldb.impl.SequenceNumber.MAX_SEQUENCE_NUMBER;
import static org.iq80.leveldb.impl.ValueType.VALUE;

// todo this class should be immutable
public class Level0
{
    private final TableCache tableCache;
    private final InternalKeyComparator internalKeyComparator;
    private final List<FileMetaData> files;

    public static final Comparator<FileMetaData> NEWEST_FIRST = new Comparator<FileMetaData>()
    {
        @Override
        public int compare(FileMetaData fileMetaData, FileMetaData fileMetaData1)
        {
            return (int) (fileMetaData1.getNumber() - fileMetaData.getNumber());
        }
    };

    public Level0(List<FileMetaData> files, TableCache tableCache, InternalKeyComparator internalKeyComparator)
    {
        Preconditions.checkNotNull(files, "files is null");
        Preconditions.checkNotNull(tableCache, "tableCache is null");
        Preconditions.checkNotNull(internalKeyComparator, "internalKeyComparator is null");

        this.files = newArrayList(files);
        this.tableCache = tableCache;
        this.internalKeyComparator = internalKeyComparator;
    }

    public int getLevelNumber()
    {
        return 0;
    }

    public List<FileMetaData> getFiles()
    {
        return files;
    }

    public LookupResult get(LookupKey key, ReadStats readStats)
            throws IOException
    {
        if (files.isEmpty()) {
            return null;
        }

        List<FileMetaData> fileMetaDataList = Lists.newArrayListWithCapacity(files.size());
        for (FileMetaData fileMetaData : files) {
            if (internalKeyComparator.getUserComparator().compare(key.getUserKey(), fileMetaData.getSmallest().getUserKey()) >= 0 &&
                    internalKeyComparator.getUserComparator().compare(key.getUserKey(), fileMetaData.getLargest().getUserKey()) <= 0) {
                fileMetaDataList.add(fileMetaData);
            }
        }

        Collections.sort(fileMetaDataList, NEWEST_FIRST);

        readStats.clear();
        for (FileMetaData fileMetaData : fileMetaDataList) {
            // open the iterator
            try (InternalIterator iterator = tableCache.newIterator(fileMetaData)) {
                // seek to the key
                iterator.seek(key.getInternalKey());

                if (iterator.hasNext()) {
                    // parse the key in the block
                    Entry<InternalKey, ByteBuffer> entry = iterator.next();
                    InternalKey internalKey = entry.getKey();
                    Preconditions.checkState(internalKey != null, "Corrupt key for %s", key.getUserKey()
                            .toString());

                    // if this is a value key (not a delete) and the keys match, return the value
                    if (key.getUserKey().equals(internalKey.getUserKey())) {
                        if (internalKey.getValueType() == ValueType.DELETION) {
                            return LookupResult.deleted(key);
                        }
                        else if (internalKey.getValueType() == VALUE) {
                            return LookupResult.ok(key, entry.getValue());
                        }
                    }
                }
            }

            if (readStats.getSeekFile() == null) {
                // We have had more than one seek for this read.  Charge the first file.
                readStats.setSeekFile(fileMetaData);
                readStats.setSeekFileLevel(0);
            }
        }

        return null;
    }

    public boolean someFileOverlapsRange(ByteBuffer smallestUserKey, ByteBuffer largestUserKey)
    {
        InternalKey smallestInternalKey = new TransientInternalKey(smallestUserKey, MAX_SEQUENCE_NUMBER, VALUE);
        int index = findFile(smallestInternalKey);

        DBBufferComparator userComparator = internalKeyComparator.getUserComparator();
        return ((index < files.size()) &&
                userComparator.compare(largestUserKey, files.get(index).getSmallest().getUserKey()) >= 0);
    }

    private int findFile(InternalKey targetKey)
    {
        if (files.isEmpty()) {
            return files.size();
        }

        // todo replace with Collections.binarySearch
        int left = 0;
        int right = files.size() - 1;

        // binary search restart positions to find the restart position immediately before the targetKey
        while (left < right) {
            int mid = (left + right) / 2;

            if (internalKeyComparator.compare(files.get(mid).getLargest(), targetKey) < 0) {
                // Key at "mid.largest" is < "target".  Therefore all
                // files at or before "mid" are uninteresting.
                left = mid + 1;
            }
            else {
                // Key at "mid.largest" is >= "target".  Therefore all files
                // after "mid" are uninteresting.
                right = mid;
            }
        }
        return right;
    }

    public void addFile(FileMetaData fileMetaData)
    {
        // todo remove mutation
        files.add(fileMetaData);
    }

    public static InternalIterator createLevel0Iterator(TableCache tableCache,
            List<FileMetaData> files,
            InternalKeyComparator internalKeyComparator)
    {
        Builder<TableIterator> builder = ImmutableList.builder();
        for (FileMetaData file : files) {
            builder.add(tableCache.newIterator(file));
        }
        return new DbIterator((MemTableIterator) null, (MemTableIterator) null, builder.build(),
                Collections.<LevelIterator> emptyList(), internalKeyComparator);
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("Level0");
        sb.append("{files=").append(files);
        sb.append('}');
        return sb.toString();
    }
}
