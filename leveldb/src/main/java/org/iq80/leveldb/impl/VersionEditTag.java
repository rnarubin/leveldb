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

import com.google.common.base.Charsets;

import org.iq80.leveldb.util.ByteBuffers;
import org.iq80.leveldb.util.GrowingBuffer;
import org.iq80.leveldb.util.VariableLengthQuantity;

import java.nio.ByteBuffer;
import java.util.Map.Entry;

public enum VersionEditTag
{
    // 8 is no longer used. It was used for large value refs.

    COMPARATOR(1)
    {
        @Override
        public void readValue(ByteBuffer buffer, VersionEdit versionEdit)
        {
            byte[] bytes = new byte[VariableLengthQuantity.readVariableLengthInt(buffer)];
            buffer.get(bytes);
            versionEdit.setComparatorName(new String(bytes, Charsets.UTF_8));
        }

        @Override
        public void writeValue(GrowingBuffer buffer, VersionEdit versionEdit)
        {
            String comparatorName = versionEdit.getComparatorName();
            if (comparatorName != null) {
                VariableLengthQuantity.writeVariableLengthInt(getPersistentId(), buffer);
                byte[] bytes = comparatorName.getBytes(Charsets.UTF_8);
                VariableLengthQuantity.writeVariableLengthInt(bytes.length, buffer);
                buffer.put(bytes);
            }
        }
    },
    LOG_NUMBER(2)
    {
        @Override
        public void readValue(ByteBuffer buffer, VersionEdit versionEdit)
        {
            versionEdit.setLogNumber(VariableLengthQuantity.readVariableLengthLong(buffer));
        }

        @Override
        public void writeValue(GrowingBuffer buffer, VersionEdit versionEdit)
        {
            Long logNumber = versionEdit.getLogNumber();
            if (logNumber != null) {
                VariableLengthQuantity.writeVariableLengthInt(getPersistentId(), buffer);
                VariableLengthQuantity.writeVariableLengthLong(logNumber, buffer);
            }
        }
    },

    PREVIOUS_LOG_NUMBER(9)
    {
        @Override
        public void readValue(ByteBuffer buffer, VersionEdit versionEdit)
        {
            long previousLogNumber = VariableLengthQuantity.readVariableLengthLong(buffer);
            versionEdit.setPreviousLogNumber(previousLogNumber);
        }

        @Override
        public void writeValue(GrowingBuffer buffer, VersionEdit versionEdit)
        {
            Long previousLogNumber = versionEdit.getPreviousLogNumber();
            if (previousLogNumber != null) {
                VariableLengthQuantity.writeVariableLengthInt(getPersistentId(), buffer);
                VariableLengthQuantity.writeVariableLengthLong(previousLogNumber, buffer);
            }
        }
    },

    NEXT_FILE_NUMBER(3)
    {
        @Override
        public void readValue(ByteBuffer buffer, VersionEdit versionEdit)
        {
            versionEdit.setNextFileNumber(VariableLengthQuantity.readVariableLengthLong(buffer));
        }

        @Override
        public void writeValue(GrowingBuffer buffer, VersionEdit versionEdit)
        {
            Long nextFileNumber = versionEdit.getNextFileNumber();
            if (nextFileNumber != null) {
                VariableLengthQuantity.writeVariableLengthInt(getPersistentId(), buffer);
                VariableLengthQuantity.writeVariableLengthLong(nextFileNumber, buffer);
            }
        }
    },

    LAST_SEQUENCE(4)
    {
        @Override
        public void readValue(ByteBuffer buffer, VersionEdit versionEdit)
        {
            versionEdit.setLastSequenceNumber(VariableLengthQuantity.readVariableLengthLong(buffer));
        }

        @Override
        public void writeValue(GrowingBuffer buffer, VersionEdit versionEdit)
        {
            Long lastSequenceNumber = versionEdit.getLastSequenceNumber();
            if (lastSequenceNumber != null) {
                VariableLengthQuantity.writeVariableLengthInt(getPersistentId(), buffer);
                VariableLengthQuantity.writeVariableLengthLong(lastSequenceNumber, buffer);
            }
        }
    },

    COMPACT_POINTER(5)
    {
        @Override
        public void readValue(ByteBuffer buffer, VersionEdit versionEdit)
        {
            // level
            int level = VariableLengthQuantity.readVariableLengthInt(buffer);

            // internal key
            // store on heap to leverage GC and make our lives easier
            InternalKey internalKey = new InternalKey(ByteBuffers.heapCopy(ByteBuffers.readLengthPrefixedBytes(buffer)));

            versionEdit.setCompactPointer(level, internalKey);
        }

        @Override
        public void writeValue(GrowingBuffer buffer, VersionEdit versionEdit)
        {
            for (Entry<Integer, InternalKey> entry : versionEdit.getCompactPointers().entrySet()) {
                VariableLengthQuantity.writeVariableLengthInt(getPersistentId(), buffer);

                // level
                VariableLengthQuantity.writeVariableLengthInt(entry.getKey(), buffer);

                // internal key
                ByteBuffers.writeLengthPrefixedBytesTransparent(buffer, entry.getValue().encode());
            }
        }
    },

    DELETED_FILE(6)
    {
        @Override
        public void readValue(ByteBuffer buffer, VersionEdit versionEdit)
        {
            // level
            int level = VariableLengthQuantity.readVariableLengthInt(buffer);

            // file number
            long fileNumber = VariableLengthQuantity.readVariableLengthLong(buffer);

            versionEdit.deleteFile(level, fileNumber);
        }

        @Override
        public void writeValue(GrowingBuffer buffer, VersionEdit versionEdit)
        {
            for (Entry<Integer, Long> entry : versionEdit.getDeletedFiles().entries()) {
                VariableLengthQuantity.writeVariableLengthInt(getPersistentId(), buffer);

                // level
                VariableLengthQuantity.writeVariableLengthInt(entry.getKey(), buffer);

                // file number
                VariableLengthQuantity.writeVariableLengthLong(entry.getValue(), buffer);
            }
        }
    },

    NEW_FILE(7)
    {
        @Override
        public void readValue(ByteBuffer buffer, VersionEdit versionEdit)
        {
            // level
            int level = VariableLengthQuantity.readVariableLengthInt(buffer);

            // file number
            long fileNumber = VariableLengthQuantity.readVariableLengthLong(buffer);

            // file size
            long fileSize = VariableLengthQuantity.readVariableLengthLong(buffer);

            // smallest key
            // store on heap to leverage GC and make our lives easier. these keys make up only a small portion of all data
            InternalKey smallestKey = new InternalKey(ByteBuffers.heapCopy(ByteBuffers.readLengthPrefixedBytes(buffer)));

            // largest key
            InternalKey largestKey = new InternalKey(ByteBuffers.heapCopy(ByteBuffers.readLengthPrefixedBytes(buffer)));

            versionEdit.addFile(level, fileNumber, fileSize, smallestKey, largestKey);
        }

        @Override
        public void writeValue(GrowingBuffer buffer, VersionEdit versionEdit)
        {
            for (Entry<Integer, FileMetaData> entry : versionEdit.getNewFiles().entries()) {
                VariableLengthQuantity.writeVariableLengthInt(getPersistentId(), buffer);

                // level
                VariableLengthQuantity.writeVariableLengthInt(entry.getKey(), buffer);

                // file number
                FileMetaData fileMetaData = entry.getValue();
                VariableLengthQuantity.writeVariableLengthLong(fileMetaData.getNumber(), buffer);

                // file size
                VariableLengthQuantity.writeVariableLengthLong(fileMetaData.getFileSize(), buffer);

                // smallest key
                ByteBuffers.writeLengthPrefixedBytesTransparent(buffer, fileMetaData.getSmallest().encode());

                // largest key
                ByteBuffers.writeLengthPrefixedBytesTransparent(buffer, fileMetaData.getLargest().encode());
            }
        }
    };

    private static final VersionEditTag[] indexedTag = new VersionEditTag[10];
    static {
        for (VersionEditTag tag : VersionEditTag.values()) {
            indexedTag[tag.persistentId] = tag;
        }
    }
    public static VersionEditTag getValueTypeByPersistentId(int persistentId)
    {
        VersionEditTag tag;
        if (persistentId < 0 || persistentId >= indexedTag.length || (tag = indexedTag[persistentId]) == null) {
            throw new IllegalArgumentException(String.format("Unknown %s persistentId %d",
                    VersionEditTag.class.getSimpleName(), persistentId));
        }
        return tag;
    }

    private final int persistentId;

    VersionEditTag(int persistentId)
    {
        this.persistentId = persistentId;
    }

    public int getPersistentId()
    {
        return persistentId;
    }

    public abstract void readValue(ByteBuffer buffer, VersionEdit versionEdit);

    public abstract void writeValue(GrowingBuffer buffer, VersionEdit versionEdit);
}
