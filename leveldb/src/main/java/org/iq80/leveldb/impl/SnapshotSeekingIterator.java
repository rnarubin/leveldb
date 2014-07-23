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

import com.google.common.collect.Maps;

import org.iq80.leveldb.util.AbstractReverseSeekingIterator;
import org.iq80.leveldb.util.DbIterator;
import org.iq80.leveldb.util.Slice;

import java.util.Comparator;
import java.util.Map.Entry;

import static org.iq80.leveldb.impl.SnapshotSeekingIterator.ValidDirection.*;

public final class SnapshotSeekingIterator extends AbstractReverseSeekingIterator<Slice, Slice>
{
   private final DbIterator iterator;
   private final SnapshotImpl snapshot;
   private final Comparator<Slice> userComparator;
   // indicates whether the iterator has been advanced to the next or previous user entry with the
// appropriate snapshot version
   private ValidDirection snapshotValidDirection = NONE;

   protected enum ValidDirection
   {
      NEXT, PREV, NONE
   }

   public SnapshotSeekingIterator(
         DbIterator iterator,
         SnapshotImpl snapshot,
         Comparator<Slice> userComparator)
   {
      this.iterator = iterator;
      this.snapshot = snapshot;
      this.userComparator = userComparator;
      this.snapshot.getVersion().retain();
   }

   public void close()
   {
      this.snapshot.getVersion().release();
   }

   @Override
   protected void seekToFirstInternal()
   {
      iterator.seekToFirst();
      snapshotValidDirection = NONE;
   }

   @Override
   protected void seekToLastInternal()
   {
      seekToEnd();
      getPrevElement();
   }
   
   @Override
   public void seekToEnd(){
      iterator.seekToEnd();
      snapshotValidDirection = NONE;
   }

   @Override
   protected void seekInternal(Slice targetKey)
   {
      iterator.seek(new InternalKey(targetKey, snapshot.getLastSequence(), ValueType.VALUE));
      snapshotValidDirection = NONE;
   }

   @Override
   protected Entry<Slice, Slice> getNextElement()
   {
      findNextUserEntry();
      

      if (!iterator.hasNext())
      {
         return null;
      }

      Entry<InternalKey, Slice> next = iterator.next();
      snapshotValidDirection = PREV;

      return Maps.immutableEntry(next.getKey().getUserKey(), next.getValue());
   }

   @Override
   protected Entry<Slice, Slice> getPrevElement()
   {
      findPrevUserEntry();

      if (!iterator.hasPrev())
      {
         return null;
      }

      Entry<InternalKey, Slice> prev = iterator.prev();
      snapshotValidDirection = NEXT;

      return Maps.immutableEntry(prev.getKey().getUserKey(), prev.getValue());
   }

   private void findNextUserEntry()
   {
      /*
       * when reverse iteration was not implemented, the snapshot iterator was always kept in a
       * state where the next entry was guaranteed to be in the appropriate version (by calling this
       * findNextUserEntry function when advancing next). With reverse iteration, however, this
       * state of validity cannot be maintained when the iterator may be arbitrarily advanced
       * forwards or backwards without excessive forward/backward advancing. Therefore, we keep a
       * record of which direction (if any) is currently at a valid position in which the following
       * entry is in the correct version
       */
      if (snapshotValidDirection == NEXT)
      {
         return;
      }

      Slice deletedKey =
            snapshotValidDirection == PREV && iterator.hasPrev()
                  ? iterator.peekPrev().getKey().getUserKey()
                  : null;

      while (iterator.hasNext())
      {
         InternalKey internalKey = iterator.peek().getKey();

         // skip entries created after our snapshot
         if (internalKey.getSequenceNumber() > snapshot.getLastSequence())
         {
            iterator.next();
            continue;
         }

         // if the next entry is a deletion, skip all subsequent entries for that key
         if (internalKey.getValueType() == ValueType.DELETION)
         {
            deletedKey = internalKey.getUserKey();
         }
         else if (internalKey.getValueType() == ValueType.VALUE)
         {
            // is this value masked by a prior deletion record?
            if (deletedKey == null
                  || userComparator.compare(internalKey.getUserKey(), deletedKey) > 0)
            {
               break;
            }
         }
         iterator.next();
      }
      // either a break from the loop, so the peek entry was valid (and next will be valid)
      // or hasNext is false: there are no items, but the direction is valid
      snapshotValidDirection = NEXT;
   }

   private void findPrevUserEntry()
   {
      if (snapshotValidDirection == PREV)
      {
         return;
      }
      Slice deletedKey =
            snapshotValidDirection == NEXT && iterator.hasNext()
                  ? iterator.peek().getKey().getUserKey()
                  : null;

      while (iterator.hasPrev())
      {
         InternalKey internalKey = iterator.peekPrev().getKey();

         // skip entries created after our snapshot
         if (internalKey.getSequenceNumber() > snapshot.getLastSequence())
         {
            iterator.prev();
            continue;
         }

         // if the next entry is a deletion, skip all subsequent entries for that key
         if (internalKey.getValueType() == ValueType.DELETION)
         {
            deletedKey = internalKey.getUserKey();
         }
         else if (internalKey.getValueType() == ValueType.VALUE)
         {
            // is this value masked by a prior deletion record?
            if (deletedKey == null
                  || userComparator.compare(internalKey.getUserKey(), deletedKey) < 0)
            {
               break;
            }
         }
         iterator.prev();
      }
      snapshotValidDirection = PREV;
   }

   @Override
   public String toString()
   {
      final StringBuilder sb = new StringBuilder();
      sb.append("SnapshotSeekingIterator");
      sb.append("{snapshot=").append(snapshot);
      sb.append(", iterator=").append(iterator);
      sb.append('}');
      return sb.toString();
   }

   @Override
   protected boolean hasNextInternal()
   {
      findNextUserEntry();
      return iterator.hasNext();
   }

   @Override
   protected boolean hasPrevInternal()
   {
      findPrevUserEntry();
      return iterator.hasPrev();
   }

}
