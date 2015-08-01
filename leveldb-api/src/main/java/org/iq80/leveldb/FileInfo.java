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

import org.iq80.leveldb.Env.DBHandle;

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
    private final DBHandle db;

    protected FileInfo(DBHandle db, FileInfo.FileType fileType)
    {
        this(db, fileType, -1);
    }

    protected FileInfo(DBHandle db, FileInfo.FileType fileType, long fileNumber)
    {
        this.fileType = fileType;
        this.fileNumber = fileNumber;
        this.db = db;
    }

    public static FileInfo log(DBHandle db, long number)
    {
        return new FileInfo(db, FileType.LOG, number);
    }

    public static FileInfo table(DBHandle db, long number)
    {
        return new FileInfo(db, FileType.TABLE, number);
    }

    public static FileInfo lock(DBHandle db)
    {
        return new FileInfo(db, FileType.DB_LOCK);
    }

    public static FileInfo current(DBHandle db)
    {
        return new FileInfo(db, FileType.CURRENT);
    }

    public static FileInfo temp(DBHandle db, long number)
    {
        return new FileInfo(db, FileType.TEMP, number);
    }

    public static FileInfo manifest(DBHandle db, long number)
    {
        return new FileInfo(db, FileType.MANIFEST, number);
    }

    public static FileInfo legacyInfoLog(DBHandle db)
    {
        return new FileInfo(db, FileType.INFO_LOG);
    }

    public FileType getFileType()
    {
        return fileType;
    }

    public long getFileNumber()
    {
        return fileNumber;
    }

    /**
     * @return a DBHandle representing the database to which this file belongs
     */
    public DBHandle getOwner()
    {
        return db;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((db == null) ? 0 : db.hashCode());
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
        if (db == null) {
            if (other.db != null)
                return false;
        }
        else if (!db.equals(other.db))
            return false;
        if (fileNumber != other.fileNumber)
            return false;
        if (fileType != other.fileType)
            return false;
        return true;
    }

    @Override
    public String toString()
    {
        return "FileInfo [fileType=" + fileType + ", fileNumber=" + fileNumber + ", db=" + db + "]";
    }
}

