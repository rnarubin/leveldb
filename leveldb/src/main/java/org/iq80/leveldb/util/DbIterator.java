
package org.iq80.leveldb.util;

import com.google.common.primitives.Ints;

import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.MemTable.MemTableIterator;
import org.iq80.leveldb.impl.ReverseSeekingIterator;

import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;

public final class DbIterator extends AbstractReverseSeekingIterator<InternalKey, Slice>
      implements
         InternalIterator
{

   /*
    * NOTE: This code has been specifically tuned for performance of the DB iterator methods. Before
    * committing changes to this code, make sure that the performance of the DB benchmark with the
    * following parameters has not regressed:
    * 
    * --num=10000000 --benchmarks=fillseq,readrandom,readseq,readseq,readseq
    * 
    * The code in this class purposely does not use the SeekingIterator interface, but instead used
    * the concrete implementations. This is because we want the hot spot compiler to inline the code
    * from the concrete iterators, and this can not happen with truly polymorphic call-sites. If a
    * future version of hot spot supports inlining of truly polymorphic call-sites, this code can be
    * made much simpler.
    */

   private final MemTableIterator memTableIterator;
   private final MemTableIterator immutableMemTableIterator;
   private final List<InternalTableIterator> level0Files;
   private final List<LevelIterator> levels;

   private final Comparator<InternalKey> comparator;

   private final DoubleHeap<OrdinalIterator> doubleHeap;

   public DbIterator(MemTableIterator memTableIterator,
         MemTableIterator immutableMemTableIterator,
         List<InternalTableIterator> level0Files,
         List<LevelIterator> levels,
         Comparator<InternalKey> comparator)
   {
      this.memTableIterator = memTableIterator;
      this.immutableMemTableIterator = immutableMemTableIterator;
      this.level0Files = level0Files;
      this.levels = levels;
      this.comparator = comparator;

      this.doubleHeap = new DoubleHeap<>(new NextElementComparator(), new PrevElementComparator());
      resetPriorityQueue();
   }

   @Override
   protected void seekToFirstInternal()
   {
      if (memTableIterator != null)
      {
         memTableIterator.seekToFirst();
      }
      if (immutableMemTableIterator != null)
      {
         immutableMemTableIterator.seekToFirst();
      }
      for (InternalTableIterator level0File : level0Files)
      {
         level0File.seekToFirst();
      }
      for (LevelIterator level : levels)
      {
         level.seekToFirst();
      }
      resetPriorityQueue();
   }

   @Override
   protected void seekToLastInternal()
   {
      if (memTableIterator != null)
      {
         memTableIterator.seekToLast();
      }
      if (immutableMemTableIterator != null)
      {
         immutableMemTableIterator.seekToLast();
      }
      for (InternalTableIterator level0File : level0Files)
      {
         level0File.seekToLast();
      }
      for (LevelIterator level : levels)
      {
         level.seekToLast();
      }
      resetPriorityQueue();
   }

   @Override
   protected void seekInternal(InternalKey targetKey)
   {
      if (memTableIterator != null)
      {
         memTableIterator.seek(targetKey);
      }
      if (immutableMemTableIterator != null)
      {
         immutableMemTableIterator.seek(targetKey);
      }
      for (InternalTableIterator level0File : level0Files)
      {
         level0File.seek(targetKey);
      }
      for (LevelIterator level : levels)
      {
         level.seek(targetKey);
      }
      resetPriorityQueue();
   }

   @Override
   protected Entry<InternalKey, Slice> getPrevElement()
   {
      if (doubleHeap.maxSize() == 0)
      {
         return null;
      }

      OrdinalIterator largest = doubleHeap.removeMax();
      Entry<InternalKey, Slice> result = largest.iterator.prev();

      // if the largest iterator has more elements, put it back in the heap,
      if (largest.iterator.hasPrev())
      {
         doubleHeap.addMax(largest);
      }

      return result;
   }

   @Override
   protected Entry<InternalKey, Slice> getNextElement()
   {
      if (doubleHeap.minSize() == 0)
      {
         return null;
      }

      OrdinalIterator smallest = doubleHeap.removeMin();
      Entry<InternalKey, Slice> result = smallest.iterator.next();

      // if the smallest iterator has more elements, put it back in the heap,
      if (smallest.iterator.hasNext())
      {
         doubleHeap.addMin(smallest);
      }

      return result;
   }

   private void resetPriorityQueue()
   {
      int i = 0;
      doubleHeap.clear();
      if (memTableIterator != null && memTableIterator.hasNext())
      {
         doubleHeap.add(new OrdinalIterator(i++, memTableIterator));
      }
      if (immutableMemTableIterator != null && immutableMemTableIterator.hasNext())
      {
         doubleHeap.add(new OrdinalIterator(i++, immutableMemTableIterator));
      }
      for (InternalTableIterator level0File : level0Files)
      {
         if (level0File.hasNext())
         {
            doubleHeap.add(new OrdinalIterator(i++, level0File));
         }
      }
      for (LevelIterator level : levels)
      {
         if (level.hasNext())
         {
            doubleHeap.add(new OrdinalIterator(i++, level));
         }
      }
   }

   @Override
   public String toString()
   {
      final StringBuilder sb = new StringBuilder();
      sb.append("DbIterator");
      sb.append("{memTableIterator=").append(memTableIterator);
      sb.append(", immutableMemTableIterator=").append(immutableMemTableIterator);
      sb.append(", level0Files=").append(level0Files);
      sb.append(", levels=").append(levels);
      sb.append(", comparator=").append(comparator);
      sb.append('}');
      return sb.toString();
   }

   private class OrdinalIterator
   {
      final public ReverseSeekingIterator<InternalKey, Slice> iterator;
      final public int ordinal;

      public OrdinalIterator(int ordinal, ReverseSeekingIterator<InternalKey, Slice> iterator)
      {
         this.ordinal = ordinal;
         this.iterator = iterator;
      }
   }

   private class NextElementComparator implements Comparator<OrdinalIterator>
   {
      @Override
      public int compare(OrdinalIterator o1, OrdinalIterator o2)
      {
         int result = comparator.compare(o1.iterator.peek().getKey(), o2.iterator.peek().getKey());
         if (result == 0)
         {
            result = Ints.compare(o1.ordinal, o2.ordinal);
         }
         return result;
      }
   }

   private class PrevElementComparator implements Comparator<OrdinalIterator>
   {
      @Override
      public int compare(OrdinalIterator o1, OrdinalIterator o2)
      {
         int result =
               comparator.compare(o1.iterator.peekPrev().getKey(), o2.iterator.peekPrev().getKey());
         if (result == 0)
         {
            result = Ints.compare(o1.ordinal, o2.ordinal);
         }
         return result;
      }

   }

}
