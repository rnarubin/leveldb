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

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import com.cleversafe.leveldb.Env;
import com.cleversafe.leveldb.FileInfo;
import com.cleversafe.leveldb.util.CompletableFutures;

/**
 * Wrapper Env used to convert abstract method arguments to concrete paths (e.g. {@link FileInfo} ->
 * {@link Path})
 */
public abstract class PathEnv implements Env {
  // whether new tables should be written with .sst extension
  private final boolean legacySST;

  public PathEnv(final boolean legacySST) {
    this.legacySST = legacySST;
  }

  @Override
  public CompletionStage<? extends ConcurrentWriteFile> openConcurrentWriteFile(
      final FileInfo fileInfo) {
    final Path path = getPath(fileInfo);
    return fileExists(path)
        .thenCompose(exists -> exists ? deleteFile(path) : CompletableFuture.completedFuture(null))
        .thenCompose(voided -> openConcurrentWriteFile(path));
  }

  @Override
  public CompletionStage<? extends SequentialWriteFile> openSequentialWriteFile(
      final FileInfo fileInfo) {
    final Path path = getPath(fileInfo);
    return fileExists(path)
        .thenCompose(exists -> exists ? deleteFile(path) : CompletableFuture.completedFuture(null))
        .thenCompose(voided -> openSequentialWriteFile(path));
  }

  @Override
  public CompletionStage<? extends TemporaryWriteFile> openTemporaryWriteFile(final FileInfo temp,
      final FileInfo target) {
    final Path tempPath = getPath(temp);
    return fileExists(tempPath)
        .thenCompose(
            exists -> exists ? deleteFile(tempPath) : CompletableFuture.completedFuture(null))
        .thenCompose(voided -> openTemporaryWriteFile(tempPath, getPath(target)));
  }

  @Override
  public CompletionStage<? extends SequentialReadFile> openSequentialReadFile(
      final FileInfo fileInfo) {
    return openSequentialReadFile(getPath(fileInfo));
  }

  @Override
  public CompletionStage<? extends RandomReadFile> openRandomReadFile(final FileInfo fileInfo) {
    final Path path = getPath(fileInfo);
    return fileExists(path).thenCompose(exists -> {
      if (exists) {
        return CompletableFuture.completedFuture(Optional.of(path));
      }
      if (legacySST) {
        return CompletableFuture.completedFuture(Optional.<Path>empty());
      }
      final Path sstPath = getPath(fileInfo, true);
      return fileExists(sstPath)
          .thenApply(sstExists -> sstExists ? Optional.of(sstPath) : Optional.<Path>empty());
    }).thenCompose(foundPath -> foundPath.isPresent() ? openRandomReadFile(foundPath.get())
        : CompletableFutures
            .exceptionalFuture(new IOException("file " + path + " does not exist")));
  }

  @Override
  public CompletionStage<? extends LockFile> lockFile(final FileInfo fileInfo) {
    return lockFile(getPath(fileInfo));
  }

  @Override
  public CompletionStage<Boolean> fileExists(final FileInfo fileInfo) {
    return fileExists(getPath(fileInfo));
  }

  @Override
  public CompletionStage<Void> deleteFile(final FileInfo fileInfo) {
    return deleteFile(getPath(fileInfo));
  }

  @Override
  public CompletionStage<DBPath> createDB(final Optional<DBHandle> handle) {
    final DBPath dbpath = pathFromHandle(handle.orElseThrow(
        () -> new IllegalArgumentException("a path is required to create the database directory")));
    return createDirIfNecessary(dbpath.path).thenApply(voidReturn -> dbpath);
  }

  private static final Predicate<Path> FILE_FILTER =
      path -> isValidFileInfo(path.getFileName().toString());

  @Override
  public CompletionStage<Void> deleteDB(final DBHandle handle) {
    return deleteDB(pathFromHandle(handle).path, FILE_FILTER);
  }

  @Override
  public CompletionStage<? extends AsynchronousCloseableIterator<FileInfo>> getOwnedFiles(
      final DBHandle handle) {
    return getOwnedFiles(pathFromHandle(handle).path, FILE_FILTER,
        path -> parseFileInfo(handle, path));
  }

  protected abstract CompletionStage<? extends ConcurrentWriteFile> openConcurrentWriteFile(
      Path path);

  protected abstract CompletionStage<? extends SequentialWriteFile> openSequentialWriteFile(
      Path path);

  protected abstract CompletionStage<? extends TemporaryWriteFile> openTemporaryWriteFile(Path temp,
      Path target);

  protected abstract CompletionStage<? extends SequentialReadFile> openSequentialReadFile(
      Path path);

  protected abstract CompletionStage<? extends RandomReadFile> openRandomReadFile(Path path);

  protected abstract CompletionStage<? extends LockFile> lockFile(Path path);

  protected abstract CompletionStage<Void> deleteFile(Path path);

  protected abstract CompletionStage<Boolean> fileExists(Path path);

  /**
   * if the dir does not exist, create the dir; if the path exists but is not a directory, throw
   * exception; if the path exists and is a directory, do nothing
   */
  protected abstract CompletionStage<Void> createDirIfNecessary(Path path);

  protected abstract CompletionStage<Void> deleteDB(Path path, Predicate<Path> fileNameFilter);

  protected abstract CompletionStage<? extends AsynchronousCloseableIterator<FileInfo>> getOwnedFiles(
      Path dir, Predicate<Path> fileNameFilter, Function<Path, FileInfo> parser);



  public static DBPath handle(final Path path) {
    return new DBPath(path);
  }

  private static DBPath pathFromHandle(final DBHandle handle) {
    if (!(handle instanceof DBPath)) {
      throw new IllegalArgumentException("given handle from different env:" + handle);
    }
    return (DBPath) handle;
  }

  private Path getPath(final FileInfo fileInfo) {
    return getPath(fileInfo, legacySST);
  }

  private static Path getPath(final FileInfo fileInfo, final boolean legacy) {
    return pathFromHandle(fileInfo.getOwner()).path.resolve(getFileName(fileInfo, legacy));
  }

  @SuppressWarnings("deprecation")
  private static String getFileName(final FileInfo fileInfo, final boolean legacy) {
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

  private final static Pattern FILE_PATTERN =
      Pattern.compile("CURRENT|LOCK|LOG(\\.old)??|MANIFEST-[0-9]+|[0-9]+\\.(log|sst|ldb|dbtmp)");

  private static boolean isValidFileInfo(final String fileName) {
    return FILE_PATTERN.matcher(fileName).matches();
  }

  private static FileInfo parseFileInfo(final DBHandle handle, final Path path) {
    // Owned filenames have the form:
    // dbname/CURRENT
    // dbname/LOCK
    // dbname/LOG
    // dbname/LOG.old
    // dbname/MANIFEST-[0-9]+
    // dbname/[0-9]+.(log|sst|dbtmp)
    final String fileName = path.getFileName().toString();
    if ("CURRENT".equals(fileName)) {
      return FileInfo.current(handle);
    } else if ("LOCK".equals(fileName)) {
      return FileInfo.lock(handle);
    } else if ("LOG".equals(fileName) || "LOG.old".equals(fileName)) {
      return FileInfo.legacyInfoLog(handle);
    } else if (fileName.startsWith("MANIFEST-")) {
      final long fileNumber = Long.parseLong(fileName.substring("MANIFEST-".length()));
      return FileInfo.manifest(handle, fileNumber);
    } else if (fileName.endsWith(".log")) {
      final long fileNumber = Long.parseLong(removeSuffix(fileName, 4));
      return FileInfo.log(handle, fileNumber);
    } else if (fileName.endsWith(".ldb") || fileName.endsWith(".sst")) {
      final long fileNumber = Long.parseLong(removeSuffix(fileName, 4));
      return FileInfo.table(handle, fileNumber);
    } else if (fileName.endsWith(".dbtmp")) {
      final long fileNumber = Long.parseLong(removeSuffix(fileName, 6));
      return FileInfo.temp(handle, fileNumber);
    }
    throw new IllegalArgumentException("Unknown file type:" + path);
  }

  private final static String removeSuffix(final String s, final int length) {
    return s.substring(0, s.length() - length);
  }

  private static class DBPath implements DBHandle {
    private final Path path;

    public DBPath(final Path path) {
      this.path = path;
    }

    @Override
    public String toString() {
      return "DBPath [" + path + "]";
    }
  }

}
