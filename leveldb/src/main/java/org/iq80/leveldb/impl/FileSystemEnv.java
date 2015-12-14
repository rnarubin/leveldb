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

import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

import org.iq80.leveldb.Env;
import org.iq80.leveldb.FileInfo;

public abstract class FileSystemEnv
        implements Env
{
    // whether new tables should be written with .sst extension
    private final boolean legacySST;

    public FileSystemEnv(boolean legacySST)
    {
        this.legacySST = legacySST;
    }

    protected Path getPath(FileInfo fileInfo)
    {
        return getPath(fileInfo, legacySST);
    }

    protected Path getPath(FileInfo fileInfo, boolean legacy)
    {
        assert fileInfo.getOwner() instanceof DBPath;
        return getPath((DBPath) fileInfo.getOwner(), fileInfo, legacy);
    }

    public static Path getPath(DBPath handle, FileInfo fileInfo, boolean legacy)
    {
        return handle.path.resolve(getFileName(fileInfo, legacy));
    }

    @SuppressWarnings("deprecation")
    protected static String getFileName(FileInfo fileInfo, boolean legacy)
    {
        switch (fileInfo.getFileType()) {
            case CURRENT:
                return "CURRENT";
            case DB_LOCK:
                return "LOCK";
            case INFO_LOG:
                return "LOG";
            case LOG:
                return String.format("%06d.log", fileInfo.getFileNumber());
            case MANIFEST:
                return String.format("MANIFEST-%06d", fileInfo.getFileNumber());
            case TABLE:
                return String.format(legacy ? "%06d.sst" : "%06d.ldb", fileInfo.getFileNumber());
            case TEMP:
                return String.format("%06d.dbtmp", fileInfo.getFileNumber());
            default:
                throw new IllegalArgumentException("Unknown file type:" + fileInfo);
        }
    }

    @Override
    public ConcurrentWriteFile openMultiWriteFile(FileInfo fileInfo)
            throws IOException
    {
        Path path = getPath(fileInfo);
        if (Files.exists(path)) {
            Files.delete(path);
        }
        return openMultiWriteFile(path);
    }

    @Override
    public SequentialWriteFile openSequentialWriteFile(FileInfo fileInfo)
            throws IOException
    {
        Path path = getPath(fileInfo);
        if (Files.exists(path)) {
            Files.delete(path);
        }
        return openSequentialWriteFile(path);
    }

    @Override
    public TemporaryWriteFile openTemporaryWriteFile(FileInfo temp, FileInfo target)
            throws IOException
    {
        Path targetPath = getPath(target);
        Path tempPath = getPath(temp);
        if (Files.exists(tempPath)) {
            Files.delete(tempPath);
        }
        return openTemporaryWriteFile(tempPath, targetPath);
    }

    @Override
    public SequentialReadFile openSequentialReadFile(FileInfo fileInfo)
            throws IOException
    {
        return openSequentialReadFile(getPath(fileInfo));
    }

    @Override
    public RandomReadFile openRandomReadFile(FileInfo fileInfo)
            throws IOException
    {
        Path path = getPath(fileInfo);
        if (!Files.exists(path)) {
            // try legacy sst
            Path sstpath = getPath(fileInfo, true);
            if (!Files.exists(sstpath)) {
                throw new IOException("file " + path + " does not exist");
            }
            path = sstpath;
        }
        return openRandomReadFile(path);
    }

    protected abstract ConcurrentWriteFile openMultiWriteFile(Path path)
            throws IOException;

    protected abstract SequentialWriteFile openSequentialWriteFile(Path path)
            throws IOException;

    protected abstract TemporaryWriteFile openTemporaryWriteFile(Path temp, Path target)
            throws IOException;

    protected abstract SequentialReadFile openSequentialReadFile(Path path)
            throws IOException;

    protected abstract RandomReadFile openRandomReadFile(Path path)
            throws IOException;

    @Override
    public void deleteFile(FileInfo fileInfo)
            throws IOException
    {
        Files.delete(getPath(fileInfo));
    }

    @Override
    public boolean fileExists(FileInfo fileInfo)
            throws IOException
    {
        return Files.exists(getPath(fileInfo));
    }

    protected static class DBPath
            implements DBHandle
    {
        private final Path path;

        public DBPath(Path path)
        {
            this.path = path;
        }

        @Override
        public String toString()
        {
            return "DBPath [path=" + path + "]";
        }
    }

    public static DBPath handle(Path path)
    {
        return new DBPath(path);
    }

    public DBPath pathFromHandle(DBHandle handle)
    {
        if (!(handle instanceof DBPath)) {
            throw new IllegalArgumentException("given handle from different env:" + handle);
        }
        return (DBPath) handle;
    }

    @Override
    public DBPath createDBDir(DBHandle handle)
            throws IOException
    {
        DBPath dbpath = pathFromHandle(handle);
        if (!Files.exists(dbpath.path)) {
            Files.createDirectories(dbpath.path);
        }
        else if (!Files.isDirectory(dbpath.path)) {
            throw new IllegalArgumentException("Database directory " + dbpath.path + " is not a directory");
        }
        // else exists and is directory, do nothing
        return dbpath;
    }

    @Override
    public void deleteDir(DBHandle handle)
            throws IOException
    {
        DBPath dbpath = pathFromHandle(handle);
        Files.delete(Files.walkFileTree(dbpath.path, new SimpleFileVisitor<Path>()
        {
            @Override
            public FileVisitResult visitFile(Path p, BasicFileAttributes attr)
                    throws IOException
            {
                Files.delete(p);
                return FileVisitResult.CONTINUE;
            }
        }));
    }

    @Override
    public Iterable<FileInfo> getOwnedFiles(DBHandle handle)
            throws IOException
    {
        DBPath dbpath = pathFromHandle(handle);
        List<FileInfo> list = new ArrayList<>();
        try (DirectoryStream<Path> dir = Files.newDirectoryStream(dbpath.path)) {
            for (Path p : dir) {
                list.add(parseFileName(handle, p));
            }
        }
        catch (DirectoryIteratorException e) {
            throw e.getCause();
        }
        return list;
    }

    private static FileInfo parseFileName(DBHandle handle, Path path)
    {
        // Owned filenames have the form:
        // dbname/CURRENT
        // dbname/LOCK
        // dbname/LOG
        // dbname/LOG.old
        // dbname/MANIFEST-[0-9]+
        // dbname/[0-9]+.(log|sst|dbtmp)
        String fileName = path.getFileName().toString();
        if ("CURRENT".equals(fileName)) {
            return FileInfo.current(handle);
        }
        else if ("LOCK".equals(fileName)) {
            return FileInfo.lock(handle);
        }
        else if ("LOG".equals(fileName) || "LOG.old".equals(fileName)) {
            return FileInfo.legacyInfoLog(handle);
        }
        else if (fileName.startsWith("MANIFEST-")) {
            long fileNumber = Long.parseLong(fileName.substring("MANIFEST-".length()));
            return FileInfo.manifest(handle, fileNumber);
        }
        else if (fileName.endsWith(".log")) {
            long fileNumber = Long.parseLong(removeSuffix(fileName, 4));
            return FileInfo.log(handle, fileNumber);
        }
        else if (fileName.endsWith(".ldb") || fileName.endsWith(".sst")) {
            long fileNumber = Long.parseLong(removeSuffix(fileName, 4));
            return FileInfo.table(handle, fileNumber);
        }
        else if (fileName.endsWith(".dbtmp")) {
            long fileNumber = Long.parseLong(removeSuffix(fileName, 6));
            return FileInfo.temp(handle, fileNumber);
        }
        throw new IllegalArgumentException("Unknown file type:" + path);
    }

    private final static String removeSuffix(String s, int length)
    {
        return s.substring(0, s.length() - length);
    }

    @Override
    public LockFile lockFile(FileInfo fileInfo)
            throws IOException
    {
        return lockFile(getPath(fileInfo));
    }

    protected abstract LockFile lockFile(Path path)
            throws IOException;

}
