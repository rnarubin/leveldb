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

public class FileInfo
{

    public enum FileType
    {
        LOG,
        DB_LOCK,
        TABLE,
        MANIFEST,
        CURRENT,
        TEMP,
        /**
         * @deprecated legacy information logging, replaced by slf4j
         */
        INFO_LOG
    }

    private final FileInfo.FileType fileType;
    private final long fileNumber;

    protected FileInfo(FileInfo.FileType fileType)
    {
        this(fileType, -1);
    }

    protected FileInfo(FileInfo.FileType fileType, long fileNumber)
    {
        this.fileType = fileType;
        this.fileNumber = fileNumber;
    }

    public static FileInfo log(long number)
    {
        return new FileInfo(FileType.LOG, number);
    }

    public static FileInfo table(long number)
    {
        return new FileInfo(FileType.TABLE, number);
    }

    private static final FileInfo LOCK = new FileInfo(FileType.DB_LOCK);
    public static FileInfo lock()
    {
        return LOCK;
    }

    private static final FileInfo CURRENT = new FileInfo(FileType.CURRENT);
    public static FileInfo current()
    {
        return CURRENT;
    }

    public static FileInfo temp(long number)
    {
        return new FileInfo(FileType.TEMP, number);
    }

    public static FileInfo manifest(long number)
    {
        return new FileInfo(FileType.MANIFEST, number);
    }

    public static FileInfo legacyInfoLog()
    {
        return new FileInfo(FileType.INFO_LOG);
    }

    public FileInfo.FileType getFileType()
    {
        return fileType;
    }

    public long getFileNumber()
    {
        return fileNumber;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (fileNumber ^ (fileNumber >>> 32));
        result = prime * result + ((fileType == null) ? 0 : fileType.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        FileInfo other = (FileInfo) obj;
        if (fileNumber != other.fileNumber)
            return false;
        if (fileType != other.fileType)
            return false;
        return true;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("FileInfo");
        sb.append("{fileType=").append(fileType);
        sb.append(", fileNumber=").append(fileNumber);
        sb.append('}');
        return sb.toString();
    }
}

