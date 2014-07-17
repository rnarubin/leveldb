
package org.iq80.leveldb.util;

import com.google.common.base.Function;

import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.MemTable.MemTableIterator;
import org.iq80.leveldb.impl.ReverseSeekingIterator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;

public final class DbIterator extends AbstractReverseSeekingIterator<InternalKey, Slice>
      implements
         InternalIterator
{

   private final List<OrdinalIterator> ordinalIterators;

   private final Comparator<InternalKey> comparator;

   private final DoubleHeap<OrdinalIterator> doubleHeap;

   public DbIterator(MemTableIterator memTableIterator,
         MemTableIterator immutableMemTableIterator,
         List<InternalTableIterator> level0Files,
         List<LevelIterator> levels,
         Comparator<InternalKey> comparator)
   {
      this.comparator = comparator;
      
      ordinalIterators = new ArrayList<>();
      int ordinal = 0;
      if(memTableIterator != null){
         ordinalIterators.add(new OrdinalIterator(ordinal++, memTableIterator));
      }
      
      if(immutableMemTableIterator != null){
         ordinalIterators.add(new OrdinalIterator(ordinal++, immutableMemTableIterator));
      }
      for (InternalTableIterator level0File : level0Files)
      {
         ordinalIterators.add(new OrdinalIterator(ordinal++, level0File));
      }
      for (LevelIterator level : levels)
      {
         ordinalIterators.add(new OrdinalIterator(ordinal++, level));
      }

      this.doubleHeap = new DoubleHeap<>(new SmallerNextElementComparator(), new LargerPrevElementComparator());
      resetPriorityQueue();
   }

   @Override
   protected void seekToFirstInternal()
   {
      for(OrdinalIterator ord:ordinalIterators){
         ord.iterator.seekToFirst();
      }
      resetPriorityQueue();
   }

   @Override
   protected void seekToLastInternal()
   {
      for(OrdinalIterator ord:ordinalIterators){
         ord.iterator.seekToLast();
      }
      resetPriorityQueue();
   }

   @Override
   protected void seekInternal(InternalKey targetKey)
   {
      for(OrdinalIterator ord:ordinalIterators){
         ord.iterator.seek(targetKey);
      }
      resetPriorityQueue();
   }

   @Override
   protected boolean hasNextInternal()
   {
      return doubleHeap.sizeMin() > 0 && doubleHeap.peekMin().iterator.hasNext();
   }

   @Override
   protected boolean hasPrevInternal()
   {
      return doubleHeap.sizeMax() > 0 && doubleHeap.peekMax().iterator.hasPrev();
   }

   @Override
   protected Entry<InternalKey, Slice> getPrevElement()
   {
      if (doubleHeap.sizeMax() == 0)
      {
         return null;
      }

      OrdinalIterator largest = doubleHeap.removeMax();
      boolean hadNext = largest.iterator.hasNext();
      Entry<InternalKey, Slice> result = largest.iterator.prev();

      // if the largest iterator has more elements, put it back in the heap,
      if (largest.iterator.hasPrev())
      {
         doubleHeap.addMax(largest);
      }
      if(!hadNext){
         doubleHeap.addMin(largest);
      }

      return result;
   }

   @Override
   protected Entry<InternalKey, Slice> getNextElement()
   {
      if (doubleHeap.sizeMin() == 0)
      {
         return null;
      }

      OrdinalIterator smallest = doubleHeap.removeMin();
      boolean hadPrev = smallest.iterator.hasPrev();
      Entry<InternalKey, Slice> result = smallest.iterator.next();

      // if the smallest iterator has more elements, put it back in the heap
      if (smallest.iterator.hasNext())
      {
         doubleHeap.addMin(smallest);
      }
      if(!hadPrev){
         //it must have a prev now because we've advanced to next
         doubleHeap.addMax(smallest);
      }

      return result;
   }

   private void resetPriorityQueue()
   {
      doubleHeap.clear();
      for(OrdinalIterator ord:ordinalIterators){
         if(ord.iterator.hasNext()){
            doubleHeap.addMin(ord);
         }
         if(ord.iterator.hasPrev()){
            doubleHeap.addMax(ord);
         }
      }
   }

   @Override
   public String toString()
   {
      final StringBuilder sb = new StringBuilder();
      sb.append("DbIterator");
      sb.append("{iterators=").append(ordinalIterators);
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

   protected class SmallerNextElementComparator extends ElementComparator
   {
      public SmallerNextElementComparator()
      {
         super(
            new Function<OrdinalIterator, Boolean>(){
               public Boolean apply(OrdinalIterator ord){
                  return ord.iterator.hasNext();
               }
            },
            new Function<OrdinalIterator, InternalKey>(){
               public InternalKey apply(OrdinalIterator ord){
                  return ord.iterator.peek().getKey();
               }
            });
      }
   }

   protected class LargerPrevElementComparator extends ElementComparator
   {
      public LargerPrevElementComparator()
      {
         super(
            new Function<OrdinalIterator, Boolean>(){
               public Boolean apply(OrdinalIterator ord){
                  return ord.iterator.hasPrev();
               }
            },
            new Function<OrdinalIterator, InternalKey>(){
               public InternalKey apply(OrdinalIterator ord){
                  return ord.iterator.peekPrev().getKey();
               }
            }
         );
      }
      
      @Override
      public int compare(OrdinalIterator o1, OrdinalIterator o2){
         return -super.compare(o1, o2); //negative for reverse comparison to get larger items
      }
   }
   
   private abstract class ElementComparator implements Comparator<OrdinalIterator>{
      private final Function<OrdinalIterator, Boolean> hasFollowing;
      private final Function<OrdinalIterator, InternalKey> peekFollowing;

      public ElementComparator(Function<OrdinalIterator, Boolean> hasFollowing, Function<OrdinalIterator, InternalKey> peekFollowing){
         this.hasFollowing = hasFollowing;
         this.peekFollowing = peekFollowing;
      }
      @Override
      public int compare(OrdinalIterator o1, OrdinalIterator o2)
      {
         if (hasFollowing.apply(o1))
         {
            if (hasFollowing.apply(o2))
            {
               //both iterators have a next element
               int result = comparator.compare(peekFollowing.apply(o1), peekFollowing.apply(o2));
               return result == 0 ? Integer.compare(o1.ordinal, o2.ordinal) : result;
            }
            return -1; //o2 does not have a next element, consider o1 less than the empty o2
         }
         if(hasFollowing.apply(o2)){
            return 1; //o1 does not have a next element, consider o2 less than the empty o1
         }
         return 0; //neither o1 nor o2 have a next element, consider them equals as empty iterators in this direction
      }
      
   }
}