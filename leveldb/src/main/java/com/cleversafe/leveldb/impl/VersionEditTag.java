/*
 * Copyright (C) 2011 the original author or authors. See the notice.md file distributed with this
 * work for additional information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.cleversafe.leveldb.impl;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.OptionalLong;

import com.cleversafe.leveldb.util.ByteBuffers;
import com.cleversafe.leveldb.util.GrowingBuffer;
import com.cleversafe.leveldb.util.VariableLengthQuantity;
import com.google.common.base.Charsets;

public enum VersionEditTag {
  // 8 is no longer used. It was used for large value refs.

  COMPARATOR(1) {
    @Override
    public void readValue(final ByteBuffer buffer, final VersionEdit versionEdit) {
      final byte[] bytes = new byte[VariableLengthQuantity.readVariableLengthInt(buffer)];
      buffer.get(bytes);
      versionEdit.setComparatorName(new String(bytes, Charsets.UTF_8));
    }

    @Override
    public void writeValue(final GrowingBuffer buffer, final VersionEdit versionEdit) {
      final String comparatorName = versionEdit.getComparatorName();
      if (comparatorName != null) {
        VariableLengthQuantity.writeVariableLengthInt(getPersistentId(), buffer);
        final byte[] bytes = comparatorName.getBytes(Charsets.UTF_8);
        VariableLengthQuantity.writeVariableLengthInt(bytes.length, buffer);
        buffer.put(bytes);
      }
    }
  },
  LOG_NUMBER(2) {
    @Override
    public void readValue(final ByteBuffer buffer, final VersionEdit versionEdit) {
      versionEdit.setLogNumber(VariableLengthQuantity.readVariableLengthLong(buffer));
    }

    @Override
    public void writeValue(final GrowingBuffer buffer, final VersionEdit versionEdit) {
      final OptionalLong logNumber = versionEdit.getLogNumber();
      if (logNumber.isPresent()) {
        VariableLengthQuantity.writeVariableLengthInt(getPersistentId(), buffer);
        VariableLengthQuantity.writeVariableLengthLong(logNumber.getAsLong(), buffer);
      }
    }
  },

  PREVIOUS_LOG_NUMBER(9) {
    @Override
    public void readValue(final ByteBuffer buffer, final VersionEdit versionEdit) {
      final long previousLogNumber = VariableLengthQuantity.readVariableLengthLong(buffer);
      versionEdit.setPreviousLogNumber(previousLogNumber);
    }

    @Override
    public void writeValue(final GrowingBuffer buffer, final VersionEdit versionEdit) {
      final OptionalLong previousLogNumber = versionEdit.getPreviousLogNumber();
      if (previousLogNumber.isPresent()) {
        VariableLengthQuantity.writeVariableLengthInt(getPersistentId(), buffer);
        VariableLengthQuantity.writeVariableLengthLong(previousLogNumber.getAsLong(), buffer);
      }
    }
  },

  NEXT_FILE_NUMBER(3) {
    @Override
    public void readValue(final ByteBuffer buffer, final VersionEdit versionEdit) {
      versionEdit.setNextFileNumber(VariableLengthQuantity.readVariableLengthLong(buffer));
    }

    @Override
    public void writeValue(final GrowingBuffer buffer, final VersionEdit versionEdit) {
      final OptionalLong nextFileNumber = versionEdit.getNextFileNumber();
      if (nextFileNumber.isPresent()) {
        VariableLengthQuantity.writeVariableLengthInt(getPersistentId(), buffer);
        VariableLengthQuantity.writeVariableLengthLong(nextFileNumber.getAsLong(), buffer);
      }
    }
  },

  LAST_SEQUENCE(4) {
    @Override
    public void readValue(final ByteBuffer buffer, final VersionEdit versionEdit) {
      versionEdit.setLastSequenceNumber(VariableLengthQuantity.readVariableLengthLong(buffer));
    }

    @Override
    public void writeValue(final GrowingBuffer buffer, final VersionEdit versionEdit) {
      final OptionalLong lastSequenceNumber = versionEdit.getLastSequenceNumber();
      if (lastSequenceNumber.isPresent()) {
        VariableLengthQuantity.writeVariableLengthInt(getPersistentId(), buffer);
        VariableLengthQuantity.writeVariableLengthLong(lastSequenceNumber.getAsLong(), buffer);
      }
    }
  },

  COMPACT_POINTER(5) {
    @Override
    public void readValue(final ByteBuffer buffer, final VersionEdit versionEdit) {
      // level
      final int level = VariableLengthQuantity.readVariableLengthInt(buffer);

      // internal key
      final InternalKey internalKey =
          new EncodedInternalKey(ByteBuffers.heapCopy(ByteBuffers.readLengthPrefixedBytes(buffer)));

      versionEdit.setCompactPointer(level, internalKey);
    }

    @Override
    public void writeValue(final GrowingBuffer buffer, final VersionEdit versionEdit) {
      final InternalKey[] compactPointers = versionEdit.getCompactPointers();
      for (int level = 0; level < compactPointers.length; level++) {
        if (compactPointers[level] != null) {
          VariableLengthQuantity.writeVariableLengthInt(getPersistentId(), buffer);

          VariableLengthQuantity.writeVariableLengthInt(level, buffer);

          buffer.writeLengthPrefixedKey(compactPointers[level]);
        }
      }
    }
  },

  DELETED_FILE(6) {
    @Override
    public void readValue(final ByteBuffer buffer, final VersionEdit versionEdit) {
      // level
      final int level = VariableLengthQuantity.readVariableLengthInt(buffer);

      // file number
      final long fileNumber = VariableLengthQuantity.readVariableLengthLong(buffer);

      versionEdit.deleteFile(level, fileNumber);
    }

    @Override
    public void writeValue(final GrowingBuffer buffer, final VersionEdit versionEdit) {
      final List<Long>[] deletedFiles = versionEdit.getDeletedFiles();
      for (int level = 0; level < deletedFiles.length; level++) {
        for (final Long fileNumber : deletedFiles[level]) {
          VariableLengthQuantity.writeVariableLengthInt(getPersistentId(), buffer);

          VariableLengthQuantity.writeVariableLengthInt(level, buffer);

          VariableLengthQuantity.writeVariableLengthLong(fileNumber, buffer);
        }
      }
    }
  },

  NEW_FILE(7) {
    @Override
    public void readValue(final ByteBuffer buffer, final VersionEdit versionEdit) {
      final int level = VariableLengthQuantity.readVariableLengthInt(buffer);

      final long fileNumber = VariableLengthQuantity.readVariableLengthLong(buffer);

      final long fileSize = VariableLengthQuantity.readVariableLengthLong(buffer);

      final InternalKey smallestKey =
          new EncodedInternalKey(ByteBuffers.readLengthPrefixedBytes(buffer)).heapCopy();

      final InternalKey largestKey =
          new EncodedInternalKey(ByteBuffers.readLengthPrefixedBytes(buffer)).heapCopy();

      versionEdit.addFile(level, fileNumber, fileSize, smallestKey, largestKey);
    }

    @Override
    public void writeValue(final GrowingBuffer buffer, final VersionEdit versionEdit) {
      final List<FileMetaData>[] newFiles = versionEdit.getNewFiles();
      for (int level = 0; level < newFiles.length; level++) {
        for (final FileMetaData file : newFiles[level]) {
          VariableLengthQuantity.writeVariableLengthInt(getPersistentId(), buffer);

          VariableLengthQuantity.writeVariableLengthInt(level, buffer);

          VariableLengthQuantity.writeVariableLengthLong(file.getNumber(), buffer);

          VariableLengthQuantity.writeVariableLengthLong(file.getFileSize(), buffer);

          buffer.writeLengthPrefixedKey(file.getSmallest());

          buffer.writeLengthPrefixedKey(file.getLargest());
        }
      }
    }
  };

  private static final VersionEditTag[] indexedTag = new VersionEditTag[10];

  static {
    for (final VersionEditTag tag : VersionEditTag.values()) {
      indexedTag[tag.persistentId] = tag;
    }
  }

  public static VersionEditTag getValueTypeByPersistentId(final int persistentId) {
    VersionEditTag tag;
    if (persistentId < 0 || persistentId >= indexedTag.length
        || (tag = indexedTag[persistentId]) == null) {
      throw new IllegalArgumentException(String.format("Unknown %s persistentId %d",
          VersionEditTag.class.getSimpleName(), persistentId));
    }
    return tag;
  }

  private final int persistentId;

  VersionEditTag(final int persistentId) {
    this.persistentId = persistentId;
  }

  public int getPersistentId() {
    return persistentId;
  }

  public abstract void readValue(ByteBuffer buffer, VersionEdit versionEdit);

  public abstract void writeValue(GrowingBuffer buffer, VersionEdit versionEdit);
}
