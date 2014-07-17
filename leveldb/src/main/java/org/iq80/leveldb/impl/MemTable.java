/**
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

import com.google.common.base.Preconditions;

import org.iq80.leveldb.util.InternalIterator;
import org.iq80.leveldb.util.Slice;
import org.testng.collections.Lists;

import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.iq80.leveldb.util.SizeOf.SIZE_OF_LONG;

public class MemTable implements SeekingIterable<InternalKey, Slice>
{
   private final ConcurrentSkipListMap<InternalKey, Slice> table;
   private final AtomicLong approximateMemoryUsage = new AtomicLong();

   public MemTable(InternalKeyComparator internalKeyComparator)
   {
      table = new ConcurrentSkipListMap<InternalKey, Slice>(internalKeyComparator);
   }

   public boolean isEmpty()
   {
      return table.isEmpty();
   }

   public long approximateMemoryUsage()
   {
      return approximateMemoryUsage.get();
   }

   public void add(long sequenceNumber, ValueType valueType, Slice key, Slice value)
   {
      Preconditions.checkNotNull(valueType, "valueType is null");
      Preconditions.checkNotNull(key, "key is null");
      Preconditions.checkNotNull(valueType, "valueType is null");

      InternalKey internalKey = new InternalKey(key, sequenceNumber, valueType);
      table.put(internalKey, value);

      approximateMemoryUsage.addAndGet(key.length() + SIZE_OF_LONG + value.length());
   }

   public LookupResult get(LookupKey key)
   {
      Preconditions.checkNotNull(key, "key is null");

      InternalKey internalKey = key.getInternalKey();
      Entry<InternalKey, Slice> entry = table.ceilingEntry(internalKey);
      if (entry == null)
      {
         return null;
      }

      InternalKey entryKey = entry.getKey();
      if (entryKey.getUserKey().equals(key.getUserKey()))
      {
         if (entryKey.getValueType() == ValueType.DELETION)
         {
            return LookupResult.deleted(key);
         }
         else
         {
            return LookupResult.ok(key, entry.getValue());
         }
      }
      return null;
   }

   @Override
   public MemTableIterator iterator()
   {
      return new MemTableIterator();
   }

   public class MemTableIterator implements InternalIterator, ReverseSeekingIterator<InternalKey, Slice>
   {

      private ReversePeekingIterator<Entry<InternalKey, Slice>> iterator;

      public MemTableIterator()
      {
         iterator = ReverseIterators.reversePeekingIterator(table.entrySet());
      }

      @Override
      public boolean hasNext()
      {
         return iterator.hasNext();
      }

      @Override
      public void seekToFirst()
      {
         iterator = ReverseIterators.reversePeekingIterator(table.entrySet());
      }

      @Override
      public void seek(InternalKey targetKey)
      {
         // previous implementation; returns a view of the map only containing keys greater than or
         // equal to the target
         // this would not be compatible with reverse iteration
         // iterator = Iterators.peekingIterator(table.tailMap(targetKey).entrySet().iterator());
         Set<Entry<InternalKey, Slice>> set = table.tailMap(targetKey).entrySet();

         // instead, find the smallest key greater than or equal to the targetKey in the table
          Entry<InternalKey, Slice> ceiling = table.ceilingEntry(targetKey);
          if(ceiling == null){ //no keys >= targetKey
             if(table.size() > 0){
                seekToLast();
             }
             return;
          }
          //then initialize the iterator at that key's location within the entryset (find the index with binary search)
         List<Entry<InternalKey, Slice>> entryList = Lists.newArrayList(table.entrySet());
         List<InternalKey> keyList = Lists.newArrayList(table.keySet());
         iterator =
               ReverseIterators.reversePeekingIterator(entryList.listIterator(Collections.binarySearch(
                     keyList, ceiling.getKey(), table.comparator())));
      }

      @Override
      public InternalEntry peek()
      {
         Entry<InternalKey, Slice> entry = iterator.peek();
         return new InternalEntry(entry.getKey(), entry.getValue());
      }

      @Override
      public InternalEntry next()
      {
         Entry<InternalKey, Slice> entry = iterator.next();
         return new InternalEntry(entry.getKey(), entry.getValue());
      }

      @Override
      public void remove()
      {
         throw new UnsupportedOperationException();
      }

      @Override
      public void seekToLast()
      {
         List<Entry<InternalKey, Slice>> entryList = Lists.newArrayList(table.entrySet());
         iterator =
               ReverseIterators.reversePeekingIterator(entryList.listIterator(entryList.size()));
      }

      @Override
      public Entry<InternalKey, Slice> peekPrev()
      {
         return iterator.peekPrev();
      }

      @Override
      public Entry<InternalKey, Slice> prev()
      {
         return iterator.prev();
      }

      @Override
      public boolean hasPrev()
      {
         return iterator.hasPrev();
      }
   }
}
