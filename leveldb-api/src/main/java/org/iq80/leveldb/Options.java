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
package org.iq80.leveldb;

import java.util.Objects;

public class Options
        implements Cloneable // shallow field-for-field Object.clone
{

    private static final Options DEFAULT_OPTIONS = OptionsUtil.populateFromProperties("leveldb.options.",
            new Options(null));

    /**
     * @deprecated use {@link Options#make()}
     */
    @Deprecated
    public Options()
    {
        this(DEFAULT_OPTIONS);
    }

    private Options(final Options that)
    {
        OptionsUtil.copyFields(Options.class, that, this);
    }

    public static Options make()
    {
        return copy(DEFAULT_OPTIONS);
    }

    public static Options copy(final Options other)
    {
        if (other == null)
            throw new IllegalArgumentException("copy target cannot be null");
        try {
            return (Options) other.clone();
        }
        catch (final CloneNotSupportedException e) {
            throw new Error(e);
        }
    }

    @Override
    public String toString()
    {
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
    private DBBufferComparator comparator = null;
    private Logger logger = null;
    private long cacheSize = 0;
    private boolean throttleLevel0 = true;
    private Env env;
    private MemoryManager memory = null;

    // it's hard to keep the api/impl distinction clean with non-null defaults.
    // this is a compromise i've made for legacy support
    @SuppressWarnings("deprecation")
    private Compression compression = CompressionType.SNAPPY;

    static void checkArgNotNull(Object value, String name)
    {
        if (value == null) {
            throw new IllegalArgumentException("The " + name + " argument cannot be null");
        }
    }

    public DBBufferComparator bufferComparator()
    {
        return comparator;
    }

    /**
     * Comparator used to define the order of keys in the table. Defaults to a
     * comparator that uses lexicographic byte-wise ordering
     * <p>
     * NOTE: The client must ensure that the comparator supplied here has the
     * same name and orders keys *exactly* the same as the comparator provided
     * to previous open calls on the same DB.
     */
    public Options bufferComparator(DBBufferComparator comparator)
    {
        this.comparator = comparator;
        return this;
    }

    /**
     * @deprecated use {@link Options#bufferComparator()}
     */
    public DBComparator comparator()
    {
        if (comparator == null)
            return null;
        if (comparator instanceof LegacyComparatorWrapper)
            return ((LegacyComparatorWrapper) comparator).comparator;
        else
            throw new IllegalStateException("requested legacy comparator when none was specified");
    }

    /**
     * @deprecated use {@link Options#bufferComparator(DBBufferComparator)}
     */
    public Options comparator(DBComparator comparator)
    {
        return bufferComparator(new LegacyComparatorWrapper(comparator));
    }

    public boolean createIfMissing()
    {
        return createIfMissing;
    }

    /**
     * If true, the database will be created if it is missing.
     */
    public Options createIfMissing(boolean createIfMissing)
    {
        this.createIfMissing = createIfMissing;
        return this;
    }

    public boolean errorIfExists()
    {
        return errorIfExists;
    }

    /**
     * If true, an error is raised if the database already exists.
     */
    public Options errorIfExists(boolean errorIfExists)
    {
        this.errorIfExists = errorIfExists;
        return this;
    }

    public boolean paranoidChecks()
    {
        return paranoidChecks;
    }

    /**
     * If true, the implementation will do aggressive checking of the data it is
     * processing and will stop early if it detects any errors. This may have
     * unforeseen ramifications: for example, a corruption of one DB entry may
     * cause a large number of entries to become unreadable or for the entire DB
     * to become unopenable.
     */
    public Options paranoidChecks(boolean paranoidChecks)
    {
        this.paranoidChecks = paranoidChecks;
        return this;
    }

    public Env env()
    {
        return env;
    }

    /**
     * Use the specified object to interact with the environment, e.g. to
     * read/write files
     */
    public Options env(Env env)
    {
        this.env = env;
        return this;
    }

    /**
     * @deprecated use <a href="http://www.slf4j.org/">SLF4J</a> bindings
     */
    @Deprecated
    public Logger logger()
    {
        return logger;
    }

    /**
     * @deprecated use <a href="http://www.slf4j.org/">SLF4J</a> bindings
     */
    @Deprecated
    public Options logger(Logger logger)
    {
        this.logger = logger;
        return this;
    }

    public int writeBufferSize()
    {
        return writeBufferSize;
    }

    /**
     * Amount of data, in bytes, to build up in memory (backed by an unsorted
     * log on disk) before converting to a sorted on-disk file.
     * <p>
     * Larger values increase performance, especially during bulk loads. Up to
     * two write buffers may be held in memory at the same time, so you may wish
     * to adjust this parameter to control memory usage. Also, a larger write
     * buffer will result in a longer recovery time the next time the database
     * is opened.
     */
    public Options writeBufferSize(int writeBufferSize)
    {
        this.writeBufferSize = writeBufferSize;
        return this;
    }

    public int maxOpenFiles()
    {
        return maxOpenFiles;
    }

    /**
     * Number of open files that are cached by the DB.
     */
    public Options maxOpenFiles(int maxOpenFiles)
    {
        this.maxOpenFiles = maxOpenFiles;
        return this;
    }

    // TODO: Block cache
    public long cacheSize()
    {
        return cacheSize;
    }

    public Options cacheSize(long cacheSize)
    {
        this.cacheSize = cacheSize;
        return this;
    }

    public int blockSize()
    {
        return blockSize;
    }

    /**
     * Approximate size of user data packed per block. Note that the block size
     * specified here corresponds to uncompressed data. The actual size of the
     * unit read from disk may be smaller if compression is enabled.
     */
    // TODO: This parameter can be changed dynamically.
    public Options blockSize(int blockSize)
    {
        this.blockSize = blockSize;
        return this;
    }

    public int blockRestartInterval()
    {
        return blockRestartInterval;
    }

    /**
     * Number of keys between restart points for delta encoding of keys. This
     * parameter can be changed dynamically. Most clients should leave this
     * parameter alone.
     */
    public Options blockRestartInterval(int blockRestartInterval)
    {
        this.blockRestartInterval = blockRestartInterval;
        return this;
    }
    
    public Compression compression(){
        return compression;
    }
    
    /**
     * Compress blocks using the specified compression algorithm.
     * 
     * @param compression
     *            may be null to indicate no compression
     */
    // TODO: This parameter can be changed dynamically.
    public Options compression(Compression compression)
    {
        if (compression != null && compression.persistentId() == 0) {
            throw new IllegalArgumentException("User specified compression may not use persistent id of 0");
        }
        this.compression = compression;
        return this;
    }

    /**
     * @deprecated use {@link Options#compression}
     */
    public CompressionType compressionType()
    {
        if (Objects.equals(compression, CompressionType.NONE))
            return CompressionType.NONE;
        if (Objects.equals(compression, CompressionType.SNAPPY))
            return CompressionType.SNAPPY;
        throw new IllegalStateException("requested legacy compression type when none was specified");
    }

    /**
     * @deprecated use {@link Options#compression(Compression)}
     */
    public Options compressionType(CompressionType compressionType)
    {
        checkArgNotNull(compressionType, "compressionType");
        return compression(compressionType);
    }

    public boolean verifyChecksums()
    {
        return verifyChecksums;
    }

    /**
     * If true, all data read from underlying storage will be verified against
     * corresponding checksums.
     */
    public Options verifyChecksums(boolean verifyChecksums)
    {
        this.verifyChecksums = verifyChecksums;
        return this;
    }

    public boolean throttleLevel0()
    {
        return throttleLevel0;
    }

    /**
     * If true, puts and deletes submitted to the DB will be throttled, and
     * eventually blocked, if the size of level 0 exceeds an internal threshold
     * (i.e. writes are being submitted faster than compaction can consolidate
     * level 0 files)
     * <p>
     * Defaults to true; set to false if willing to degrade read and iteration
     * performance in order to improve high-throughput write performance
     */
    public Options throttleLevel0(boolean throttleLevel0)
    {
        this.throttleLevel0 = throttleLevel0;
        return this;
    }

    public MemoryManager memoryManager()
    {
        return memory;
    }

    // TODO doc and boolean internal/external
    public Options memoryManager(MemoryManager memory)
    {
        this.memory = memory;
        return this;
    }
}
