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
package org.iq80.leveldb.util;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Closeables
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Closeables.class);
    private Closeables()
    {
    }

    public static void closeQuietly(AutoCloseable closeable)
    {
        try {
            close(closeable);
        }
        catch (Exception ignored) {
            LOGGER.warn("exception in closing {} ", closeable, ignored);
        }
    }

    public static void close(AutoCloseable closeable)
            throws Exception
    {
        if (closeable != null) {
            closeable.close();
        }
    }

    public static void closeIO(AutoCloseable closeable)
            throws IOException
    {
        try {
            close(closeable);
        }
        catch (Exception e) {
            throw new IOException(e);
        }
    }

    public static void closeIO(AutoCloseable c1, AutoCloseable c2)
            throws IOException
    {
        try {
            closeIO(c1);
        }
        finally {
            closeIO(c2);
        }
    }

    public static void closeIO(AutoCloseable... closeables)
            throws IOException
    {
        closeIO(closeables.length - 1, closeables);
    }

    private static void closeIO(int index, AutoCloseable[] closeables)
            throws IOException
    {
        int i = index;
        try {
            for (; i >= 0; i--) {
                closeIO(closeables[i]);
            }
        }
        finally {
            if (i > 0) {
                closeIO(i - 1, closeables);
            }
        }
    }
}
