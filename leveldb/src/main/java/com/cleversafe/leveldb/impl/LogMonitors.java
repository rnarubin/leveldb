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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LogMonitors {
  private LogMonitors() {}

  private static final Logger LOGGER = LoggerFactory.getLogger(LogMonitors.class);
  private static final LogMonitor EXCEPTION = new LogMonitor() {
    @Override
    public void corruption(final long bytes, final String reason) throws IOException {
      throw new IOException(String.format("corruption of %s bytes: %s", bytes, reason));
    }

    @Override
    public void corruption(final long bytes, final Throwable reason) throws IOException {
      throw new IOException(String.format("corruption of %s bytes", bytes), reason);
    }
  };
  private static final LogMonitor LOGGING = new LogMonitor() {
    @Override
    public void corruption(final long bytes, final String reason) {
      LOGGER.warn("corruption of {} bytes: {}", bytes, reason);
    }

    @Override
    public void corruption(final long bytes, final Throwable reason) {
      LOGGER.warn("corruption of {} bytes", bytes, reason);
    }
  };



  public static LogMonitor throwExceptionMonitor() {
    return EXCEPTION;
  }

  public static LogMonitor loggingMonitor() {
    return LOGGING;
  }
}
