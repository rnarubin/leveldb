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
package com.cleversafe.leveldb.impl;

import org.iq80.leveldb.DBException;
import org.testng.annotations.Test;

import com.cleversafe.leveldb.DB;
import com.cleversafe.leveldb.DBFactory;
import com.cleversafe.leveldb.Options;
import com.cleversafe.leveldb.impl.Iq80DBFactory;
import com.cleversafe.leveldb.impl.DbImplTest.StrictEnv;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.testng.Assert.assertTrue;

/**
 * Test the implementation via the com.cleversafe.leveldb API.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class ApiTest
{
    public static byte[] bytes(String value)
    {
        if (value == null) {
            return null;
        }
        try {
            return value.getBytes("UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static String asString(byte[] value)
    {
        if (value == null) {
            return null;
        }
        try {
            return new String(value, "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public void assertEquals(byte[] arg1, byte[] arg2)
    {
        assertTrue(Arrays.equals(arg1, arg2), asString(arg1) + " != " + asString(arg2));
    }

    private final DBFactory factory = Iq80DBFactory.factory;

    @Test
    public void testCompaction()
            throws IOException, DBException
    {
        try (StrictEnv env = new StrictEnv()) {
            Options options = Options.make()
                    .createIfMissing(true)
                    .compression(null)
                    .env(env)
                    .memoryManager(env.strictMemory);

            Path tempDir = Files.createTempDirectory("leveldb");
            DB db = factory.open(tempDir.toFile(), options);

            System.out.println("Adding");
            for (int i = 0; i < 1000 * 1000; i++) {
                if (i % 100000 == 0) {
                    System.out.println("  at: " + i);
                }
                db.put(bytes("key" + i), bytes("value" + i));
            }

            db.close();
            db = factory.open(tempDir.toFile(), options);

            System.out.println("Deleting");
            for (int i = 0; i < 1000 * 1000; i++) {
                if (i % 100000 == 0) {
                    System.out.println("  at: " + i);
                }
                db.delete(bytes("key" + i));
            }

            db.close();
            db = factory.open(tempDir.toFile(), options);

            System.out.println("Adding");
            for (int i = 0; i < 1000 * 1000; i++) {
                if (i % 100000 == 0) {
                    System.out.println("  at: " + i);
                }
                db.put(bytes("key" + i), bytes("value" + i));
            }

            db.close();
        }
    }
}
