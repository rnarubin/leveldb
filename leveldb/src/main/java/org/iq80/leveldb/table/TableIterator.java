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
package org.iq80.leveldb.table;

import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.util.TwoStageIterator;

import java.io.IOException;
import java.nio.ByteBuffer;

public final class TableIterator
        extends TwoStageIterator<BlockIterator<InternalKey>, BlockIterator<InternalKey>, ByteBuffer>
{
    private final Table table;

    public TableIterator(Table table, BlockIterator<InternalKey> indexIterator)
    {
        super(indexIterator);
        this.table = table;
    }

    @Override
    public void close()
            throws IOException
    {
        try {
            super.close();
        }
        finally {
            table.close();
        }
    }

    protected BlockIterator<InternalKey> getData(ByteBuffer blockHandle)
    {
        try (Block<InternalKey> dataBlock = table.openBlock(blockHandle)) {
            return dataBlock.iterator(); // dataBlock retained by iterator
        }
    }
}
