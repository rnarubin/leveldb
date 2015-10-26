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
package com.cleversafe.leveldb;

import java.util.Objects;

import com.cleversafe.leveldb.impl.FileEnv;
import com.cleversafe.leveldb.table.BytewiseComparator;
import com.cleversafe.leveldb.util.Snappy;

public class Options implements Cloneable // shallow field-for-field Object.clone
{
  private static final Options DEFAULT_OPTIONS =
      OptionsUtil.populateFromProperties("leveldb.options.", new Options(null));

  private Options(final Options that) {
    OptionsUtil.copyFields(Options.class, that, this);
  }

  public static Options make() {
    return copy(DEFAULT_OPTIONS);
  }

  public static Options copy(final Options other) {
    try {
      return (Options) Objects.requireNonNull(other, "copy target cannot be null").clone();
    } catch (final CloneNotSupportedException e) {
      throw new Error(e);
    }
  }

  @Override
  public String toString() {
    return OptionsUtil.toString(this);
  }

  private boolean createIfMissing = true;
  private boolean errorIfExists = false;
  private int writeBufferSize = 4 << 20;
  private int maxOpenFiles = 1000;
  private int blockRestartInterval = 16;
  private int blockSize = 4 * 1024;
  private boolean verifyChecksums = true;
  private boolean paranoidChecks = false;
  private DBComparator comparator = BytewiseComparator.instance();
  private long cacheSize = 0;
  private boolean throttleLevel0 = true;
  private Env env = new FileEnv();
  private Compression compression = Snappy.instance();

  /**
   * Comparator used to define the order of keys in the table. Defaults to a comparator that uses
   * lexicographic byte-wise ordering
   * <p>
   * NOTE: The client must ensure that the comparator supplied here has the same name and orders
   * keys *exactly* the same as the comparator provided to previous open calls on the same DB.
   */
  public Options bufferComparator(final DBComparator comparator) {
    this.comparator = Objects.requireNonNull(comparator, "comparator cannot be null");
    return this;
  }

  public DBComparator bufferComparator() {
    return comparator;
  }

  /**
   * If true, the database will be created if it is missing.
   */
  public Options createIfMissing(final boolean createIfMissing) {
    this.createIfMissing = createIfMissing;
    return this;
  }

  public boolean createIfMissing() {
    return createIfMissing;
  }

  /**
   * If true, an error is raised if the database already exists.
   */
  public Options errorIfExists(final boolean errorIfExists) {
    this.errorIfExists = errorIfExists;
    return this;
  }

  public boolean errorIfExists() {
    return errorIfExists;
  }

  /**
   * If true, the implementation will do aggressive checking of the data it is processing and will
   * stop early if it detects any errors. This may have unforeseen ramifications: for example, a
   * corruption of one DB entry may cause a large number of entries to become unreadable or for the
   * entire DB to become unopenable.
   */
  public Options paranoidChecks(final boolean paranoidChecks) {
    this.paranoidChecks = paranoidChecks;
    return this;
  }

  public boolean paranoidChecks() {
    return paranoidChecks;
  }

  /**
   * Use the specified object to interact with the environment, e.g. to read/write files
   */
  public Options env(final Env env) {
    this.env = Objects.requireNonNull(env, "env cannot be null");
    return this;
  }

  public Env env() {
    return env;
  }

  /**
   * Amount of data, in bytes, to build up in memory (backed by an unsorted log on disk) before
   * converting to a sorted on-disk file.
   * <p>
   * Larger values increase performance, especially during bulk loads. Up to two write buffers may
   * be held in memory at the same time, so you may wish to adjust this parameter to control memory
   * usage. Also, a larger write buffer will result in a longer recovery time the next time the
   * database is opened.
   */
  public Options writeBufferSize(final int writeBufferSize) {
    this.writeBufferSize = writeBufferSize;
    return this;
  }

  public int writeBufferSize() {
    return writeBufferSize;
  }

  /**
   * Number of open files that are cached by the DB.
   */
  public Options maxOpenFiles(final int maxOpenFiles) {
    this.maxOpenFiles = maxOpenFiles;
    return this;
  }

  public int maxOpenFiles() {
    return maxOpenFiles;
  }

  public Options cacheSize(final long cacheSize) {
    this.cacheSize = cacheSize;
    return this;
  }

  // TODO: Block cache
  public long cacheSize() {
    return cacheSize;
  }

  /**
   * Approximate size of user data packed per block. Note that the block size specified here
   * corresponds to uncompressed data. The actual size of the unit read from disk may be smaller if
   * compression is enabled.
   */
  // TODO: This parameter can be changed dynamically.
  public Options blockSize(final int blockSize) {
    this.blockSize = blockSize;
    return this;
  }

  public int blockSize() {
    return blockSize;
  }

  /**
   * Number of keys between restart points for delta encoding of keys. This parameter can be changed
   * dynamically. Most clients should leave this parameter alone.
   */
  public Options blockRestartInterval(final int blockRestartInterval) {
    this.blockRestartInterval = blockRestartInterval;
    return this;
  }

  public int blockRestartInterval() {
    return blockRestartInterval;
  }

  /**
   * Compress blocks using the specified compression algorithm.
   *
   * @param compression may be null to indicate no compression
   */
  // TODO: This parameter can be changed dynamically.
  public Options compression(final Compression compression) {
    if (compression != null && compression.persistentId() == 0) {
      throw new IllegalArgumentException(
          "User specified compression may not use persistent id of 0");
    }
    this.compression = compression;
    return this;
  }

  public Compression compression() {
    return compression;
  }

  /**
   * If true, all data read from underlying storage will be verified against corresponding
   * checksums.
   */
  public Options verifyChecksums(final boolean verifyChecksums) {
    this.verifyChecksums = verifyChecksums;
    return this;
  }

  public boolean verifyChecksums() {
    return verifyChecksums;
  }

  /**
   * If true, puts and deletes submitted to the DB will be throttled, and eventually blocked, if the
   * size of level 0 exceeds an internal threshold (i.e. writes are being submitted faster than
   * compaction can consolidate level 0 files)
   * <p>
   * Defaults to true; set to false if willing to degrade read and iteration performance in order to
   * improve high-throughput write performance
   */
  public Options throttleLevel0(final boolean throttleLevel0) {
    this.throttleLevel0 = throttleLevel0;
    return this;
  }

  public boolean throttleLevel0() {
    return throttleLevel0;
  }
}
