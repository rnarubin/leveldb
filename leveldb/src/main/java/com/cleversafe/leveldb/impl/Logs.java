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
import java.util.concurrent.CompletionStage;

import com.cleversafe.leveldb.Env;
import com.cleversafe.leveldb.FileInfo;
import com.cleversafe.leveldb.util.ByteBufferCrc32C;
import com.cleversafe.leveldb.util.ByteBuffers;

public final class Logs {
  private Logs() {}

  public static CompletionStage<LogWriter> createLogWriter(final FileInfo fileInfo, final Env env) {
    return env.openConcurrentWriteFile(fileInfo)
        .thenApply(file -> new LogWriter(file, fileInfo.getFileNumber()));
  }

  public static int getChunkChecksum(final int chunkTypeId, final ByteBuffer data) {
    final ByteBufferCrc32C crc32c = ByteBuffers.crc32c();
    crc32c.update(chunkTypeId);
    crc32c.update(data, data.position(), data.remaining());
    return ByteBuffers.maskChecksum(crc32c.getIntValue());
  }
}
