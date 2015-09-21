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
package org.iq80.leveldb.util;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.iq80.leveldb.AsynchronousCloseable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Closeables {
  private static final Logger LOGGER = LoggerFactory.getLogger(Closeables.class);

  private Closeables() {}

  public static void closeQuietly(final AutoCloseable closeable) {
    try {
      close(closeable);
    } catch (final Exception ignored) {
      LOGGER.warn("exception in closing {} ", closeable, ignored);
    }
  }

  public static CompletionStage<Void> asyncClose(final AsynchronousCloseable closeable) {
    return closeable != null ? closeable.asyncClose() : CompletableFuture.completedFuture(null);
  }

  public static CompletionStage<Void> asyncClose(
      final Optional<? extends AsynchronousCloseable> closeable) {
    return closeable.isPresent() ? closeable.get().asyncClose()
        : CompletableFuture.completedFuture(null);
  }

  public static void close(final AutoCloseable closeable) throws Exception {
    if (closeable != null) {
      closeable.close();
    }
  }

  public static void closeIO(final AutoCloseable closeable) throws IOException {
    try {
      close(closeable);
    } catch (final Exception e) {
      throw new IOException(e);
    }
  }

  public static void closeIO(final AutoCloseable c1, final AutoCloseable c2) throws IOException {
    try {
      closeIO(c1);
    } finally {
      closeIO(c2);
    }
  }

  public static void closeIO(final AutoCloseable... closeables) throws IOException {
    closeIO(closeables.length - 1, closeables);
  }

  private static void closeIO(final int index, final AutoCloseable[] closeables)
      throws IOException {
    int i = index;
    try {
      for (; i >= 0; i--) {
        closeIO(closeables[i]);
      }
    } finally {
      if (i > 0) {
        closeIO(i - 1, closeables);
      }
    }
  }

  public static void closeIO(final List<AutoCloseable> closeables) throws IOException {
    closeIO(closeables.size() - 1, closeables);
  }

  private static void closeIO(final int index, final List<AutoCloseable> closeables)
      throws IOException {
    int i = index;
    try {
      for (; i >= 0; i--) {
        closeIO(closeables.get(i));
      }
    } finally {
      if (i > 0) {
        closeIO(i - 1, closeables);
      }
    }
  }
}
