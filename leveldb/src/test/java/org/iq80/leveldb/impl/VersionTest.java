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

package org.iq80.leveldb.impl;

import static org.iq80.leveldb.impl.DbConstants.NUM_LEVELS;

import org.iq80.leveldb.table.TestUtils;
import org.iq80.leveldb.util.EnvDependentTest;
import org.iq80.leveldb.util.FileEnvTestProvider;
import org.testng.Assert;
import org.testng.annotations.Test;

public abstract class VersionTest extends EnvDependentTest {

  @Test
  public void testOverlapAssertion() {
    final FileMetaData[] nonOverlappingLevel = new FileMetaData[] {
        new FileMetaData(0, 0, TestUtils.createInternalKey("a", 0),
            TestUtils.createInternalKey("b", 0)),
        new FileMetaData(0, 0, TestUtils.createInternalKey("c", 0),
            TestUtils.createInternalKey("d", 0))};
    final FileMetaData[] overlappingLevel = new FileMetaData[] {
        new FileMetaData(0, 0, TestUtils.createInternalKey("a", 0),
            TestUtils.createInternalKey("c", 0)),
        new FileMetaData(0, 0, TestUtils.createInternalKey("b", 0),
            TestUtils.createInternalKey("d", 0))};

    for (int i = 0; i < NUM_LEVELS; i++) {
      final FileMetaData[][] files = new FileMetaData[NUM_LEVELS][];
      files[0] = overlappingLevel; // level 0 may always overlap
      for (int j = 1; j < files.length; j++) {
        files[j] = nonOverlappingLevel;
      }
      files[i] = overlappingLevel;

      final Version v = new Version(files, 0, 0, null, TestUtils.keyComparator);

      if (i == 0) {
        Assert.assertTrue(v.assertNoOverlappingFiles());
      } else {
        try {
          v.assertNoOverlappingFiles();
          Assert.fail("expected an exception");
        } catch (final Exception expected) {
        }
      }
    }

  }


  public static class FileVersionTest extends VersionTest implements FileEnvTestProvider {
  }
}


