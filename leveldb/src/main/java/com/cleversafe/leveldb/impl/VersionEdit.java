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

import static com.cleversafe.leveldb.impl.DbConstants.NUM_LEVELS;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

import com.cleversafe.leveldb.util.GrowingBuffer;
import com.cleversafe.leveldb.util.MemoryManager;
import com.cleversafe.leveldb.util.VariableLengthQuantity;

public class VersionEdit {
  private String comparatorName;
  private OptionalLong logNumber = OptionalLong.empty();
  private OptionalLong nextFileNumber = OptionalLong.empty();
  private OptionalLong previousLogNumber = OptionalLong.empty();
  private OptionalLong lastSequenceNumber = OptionalLong.empty();
  private final InternalKey[] compactPointers = new InternalKey[NUM_LEVELS];
  private final List<FileMetaData>[] newFiles;
  private final List<Long>[] deletedFiles;

  @SuppressWarnings("unchecked")
  public VersionEdit() {
    this.newFiles = new List[NUM_LEVELS];
    this.deletedFiles = new List[NUM_LEVELS];
    for (int i = 0; i < NUM_LEVELS; i++) {
      newFiles[i] = new ArrayList<>();
      deletedFiles[i] = new ArrayList<>();
    }
  }

  public VersionEdit(final ByteBuffer buffer) {
    this();
    while (buffer.hasRemaining()) {
      final int i = VariableLengthQuantity.readVariableLengthInt(buffer);
      final VersionEditTag tag = VersionEditTag.getValueTypeByPersistentId(i);
      tag.readValue(buffer, this);
    }
  }

  public String getComparatorName() {
    return comparatorName;
  }

  public void setComparatorName(final String comparatorName) {
    this.comparatorName = comparatorName;
  }

  public OptionalLong getLogNumber() {
    return logNumber;
  }

  public void setLogNumber(final long logNumber) {
    this.logNumber = OptionalLong.of(logNumber);
  }

  public OptionalLong getNextFileNumber() {
    return nextFileNumber;
  }

  public void setNextFileNumber(final long nextFileNumber) {
    this.nextFileNumber = OptionalLong.of(nextFileNumber);
  }

  public OptionalLong getPreviousLogNumber() {
    return previousLogNumber;
  }

  public void setPreviousLogNumber(final long previousLogNumber) {
    this.previousLogNumber = OptionalLong.of(previousLogNumber);
  }

  public OptionalLong getLastSequenceNumber() {
    return lastSequenceNumber;
  }

  public void setLastSequenceNumber(final long lastSequenceNumber) {
    this.lastSequenceNumber = OptionalLong.of(lastSequenceNumber);
  }

  public InternalKey[] getCompactPointers() {
    return compactPointers;
  }

  public void setCompactPointer(final int level, final InternalKey key) {
    compactPointers[level] = key;
  }

  public void setCompactPointers(final InternalKey[] givenPointers) {
    mergeCompactPointers(givenPointers, compactPointers);
  }

  public static void mergeCompactPointers(final InternalKey[] src, final InternalKey[] dst) {
    assert dst.length >= src.length;
    for (int level = 0; level < src.length; level++) {
      if (src[level] != null) {
        dst[level] = src[level];
      }
    }
  }

  public List<FileMetaData>[] getNewFiles() {
    return newFiles;
  }

  // Add the specified file at the specified level.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  public void addFile(final int level, final long fileNumber, final long fileSize,
      final InternalKey smallest, final InternalKey largest) {
    final FileMetaData fileMetaData = new FileMetaData(fileNumber, fileSize, smallest, largest);
    addFile(level, fileMetaData);
  }

  public void addFile(final int level, final FileMetaData fileMetaData) {
    newFiles[level].add(fileMetaData);
  }

  public void addFiles(final FileMetaData[][] givenFiles) {
    assert newFiles.length >= givenFiles.length;
    for (int level = 0; level < givenFiles.length; level++) {
      for (final FileMetaData fileMetaData : givenFiles[level]) {
        addFile(level, fileMetaData);
      }
    }
  }

  public List<Long>[] getDeletedFiles() {
    return deletedFiles;
  }

  // Delete the specified "file" from the specified "level".
  public void deleteFile(final int level, final long fileNumber) {
    deletedFiles[level].add(fileNumber);
  }

  public GrowingBuffer encode(final MemoryManager memory) {
    final GrowingBuffer buffer = new GrowingBuffer(4096, memory);
    for (final VersionEditTag versionEditTag : VersionEditTag.values()) {
      versionEditTag.writeValue(buffer, this);
    }
    return buffer;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("VersionEdit");
    sb.append("{comparatorName='").append(comparatorName).append('\'');
    sb.append(", logNumber=").append(logNumber);
    sb.append(", previousLogNumber=").append(previousLogNumber);
    sb.append(", lastSequenceNumber=").append(lastSequenceNumber);
    sb.append(", compactPointers=").append(compactPointers);
    sb.append(", newFiles=").append(newFiles);
    sb.append(", deletedFiles=").append(deletedFiles);
    sb.append('}');
    return sb.toString();
  }
}
