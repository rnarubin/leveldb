/**
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

public class Options {

    private boolean createIfMissing = true;
    private boolean errorIfExists;
    private int writeBufferSize = 4 << 20;

    private int maxOpenFiles = 1000;

    private int blockRestartInterval = 16;
    private int blockSize = 4 * 1024;
    private CompressionType compressionType = CompressionType.SNAPPY;
    private boolean verifyChecksums = true;
    private boolean paranoidChecks = false;
    private DBComparator comparator;
    private Logger logger = null;
    private long cacheSize;

    public static final int CPU_DATA_MODEL = Integer.getInteger("sun.arch.data.model");

    // We only use MMAP on 64 bit systems since it's really easy to run out of
    // virtual address space on a 32 bit system when all the data is getting mapped
    // into memory.  If you really want to use MMAP anyways, use -Dleveldb.mmap=true
    public static final boolean USE_MMAP_DEFAULT = Boolean.parseBoolean(System.getProperty("leveldb.mmap", ""+(CPU_DATA_MODEL>32)));
    private boolean useMMap = USE_MMAP_DEFAULT;

    static void checkArgNotNull(Object value, String name) {
        if(value==null) {
            throw new IllegalArgumentException("The "+name+" argument cannot be null");
        }
    }

    public boolean createIfMissing()
    {
        return createIfMissing;
    }

    public Options createIfMissing(boolean createIfMissing)
    {
        this.createIfMissing = createIfMissing;
        return this;
    }

    public boolean errorIfExists()
    {
        return errorIfExists;
    }

    public Options errorIfExists(boolean errorIfExists)
    {
        this.errorIfExists = errorIfExists;
        return this;
    }

    public int writeBufferSize()
    {
        return writeBufferSize;
    }

    public Options writeBufferSize(int writeBufferSize)
    {
        this.writeBufferSize = writeBufferSize;
        return this;
    }

    public int maxOpenFiles()
    {
        return maxOpenFiles;
    }

    public Options maxOpenFiles(int maxOpenFiles)
    {
        this.maxOpenFiles = maxOpenFiles;
        return this;
    }

    public int blockRestartInterval()
    {
        return blockRestartInterval;
    }

    public Options blockRestartInterval(int blockRestartInterval)
    {
        this.blockRestartInterval = blockRestartInterval;
        return this;
    }

    public int blockSize()
    {
        return blockSize;
    }

    public Options blockSize(int blockSize)
    {
        this.blockSize = blockSize;
        return this;
    }

    public CompressionType compressionType()
    {
        return compressionType;
    }

    public Options compressionType(CompressionType compressionType)
    {
        checkArgNotNull(compressionType, "compressionType");
        this.compressionType = compressionType;
        return this;
    }

    public boolean verifyChecksums()
    {
        return verifyChecksums;
    }

    public Options verifyChecksums(boolean verifyChecksums)
    {
        this.verifyChecksums = verifyChecksums;
        return this;
    }


    public long cacheSize() {
        return cacheSize;
    }

    public Options cacheSize(long cacheSize) {
        this.cacheSize = cacheSize;
        return this;
    }

    public DBComparator comparator() {
        return comparator;
    }

    public Options comparator(DBComparator comparator) {
        this.comparator = comparator;
        return this;
    }

    public Logger logger() {
        return logger;
    }

    public Options logger(Logger logger) {
        this.logger = logger;
        return this;
    }

    public boolean paranoidChecks() {
        return paranoidChecks;
    }

    public Options paranoidChecks(boolean paranoidChecks) {
        this.paranoidChecks = paranoidChecks;
        return this;
    }

    public boolean useMMap(){
       return useMMap;
    }

    public Options useMMap(boolean useMMap){
       this.useMMap = useMMap;
       return this;
    }
}
