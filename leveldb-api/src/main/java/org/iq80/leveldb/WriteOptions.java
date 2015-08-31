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

public class WriteOptions
        implements Cloneable
{
    private static final WriteOptions DEFAULT_WRITE_OPTIONS = OptionsUtil.populateFromProperties(
            "leveldb.writeOptions.", new WriteOptions(null));

    /**
     * @deprecated use {@link WriteOptions#make()}
     */
    public WriteOptions()
    {
        this(DEFAULT_WRITE_OPTIONS);
    }

    private WriteOptions(WriteOptions that)
    {
        OptionsUtil.copyFields(WriteOptions.class, that, this);
    }

    public static WriteOptions make()
    {
        return copy(DEFAULT_WRITE_OPTIONS);
    }

    public static WriteOptions copy(WriteOptions other)
    {
        if (other == null)
            throw new IllegalArgumentException("copy target cannot be null");
        try {
            return (WriteOptions) other.clone();
        }
        catch (CloneNotSupportedException e) {
            return new WriteOptions(DEFAULT_WRITE_OPTIONS);
        }
    }

    @Override
    public String toString()
    {
        return OptionsUtil.toString(this);
    }

    private boolean sync = false;
    private boolean snapshot = false;
    private boolean disableLog = false;

    public boolean sync()
    {
        return sync;
    }

    /**
     * If true, the write will be flushed from associated buffer caches (by
     * calling {@link org.iq80.leveldb.Env.ConcurrentWriteFile.WriteRegion#sync
     * sync}) before the write is considered complete. If this flag is true,
     * writes will be slower.
     * <p>
     * If this flag is false, and the machine crashes, some recent writes may be
     * lost.
     */
    public WriteOptions sync(boolean sync)
    {
        this.sync = sync;
        return this;
    }

    public boolean snapshot()
    {
        return snapshot;
    }

    /**
     * If true, return the first snapshot that includes this write when
     * returning from the put
     */
    public WriteOptions snapshot(boolean snapshot)
    {
        this.snapshot = snapshot;
        return this;
    }

    public boolean disableLog()
    {
        return disableLog;
    }

    /**
     * If true, the write will not be written to the persisted log, and will
     * only be written to media when the encompassing memtable is flushed.
     * <p>
     * In order to preserve data in the event of an orderly shutdown (one in
     * which {@link DB#close()} is called), closing may take longer when this
     * flag is enabled
     * <p>
     * If this flag is enabled and the application crashes, this write may be
     * lost
     */
    public WriteOptions disableLog(boolean disableLog)
    {
        this.disableLog = disableLog;
        return this;
    }
}
