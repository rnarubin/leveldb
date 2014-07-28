
package org.iq80.leveldb.util;

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

   private final ArrayList<OrdinalIterator> ordinalIterators;
   private final Comparator<OrdinalIterator> smallerNext, largerPrev;

   private final Comparator<InternalKey> comparator;

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
      
      smallerNext = new SmallerNextElementComparator();
      largerPrev = new LargerPrevElementComparator();

   }

   @Override
   protected void seekToFirstInternal()
   {
      for(OrdinalIterator ord:ordinalIterators){
         ord.iterator.seekToFirst();
      }
   }

   @Override
   protected void seekToLastInternal()
   {
      for(OrdinalIterator ord:ordinalIterators){
         ord.iterator.seekToEnd();
      }
      getPrevElement();
   }
   
   @Override
   public void seekToEndInternal(){
       for(OrdinalIterator ord:ordinalIterators){
         ord.iterator.seekToEnd();
       }
   }


   @Override
   protected void seekInternal(InternalKey targetKey)
   {
      for(OrdinalIterator ord:ordinalIterators){
         ord.iterator.seek(targetKey);
      }
   }
   
   private ReverseSeekingIterator<InternalKey, Slice> getMin(){
      /*
       * in the DoubleHeap approach it proved difficult to coordinate the two heaps when reverse iteration is necessary
       * instead, just perform linear search of the iterators for the min or max item
       * there tends to be only a small number of iterators (less than 10) even when the database contains a substantial number of items (in the millions)
       * (the c++ implementation, as of this writing, also uses linear search)
       */
      OrdinalIterator min = ordinalIterators.get(0);
      
      for(int i = 1; i < ordinalIterators.size(); i++){
         OrdinalIterator ord = ordinalIterators.get(i);
         if(ord.iterator.hasNext() 
               && (!min.iterator.hasNext() 
                     || smallerNext.compare(ord, min) < 0)){
            //if the following iterator hasNext (or hasPrev, larger, etc.) and the current minimum does not
            //or if the following is simply smaller
            //we've found a new minimum
            min = ord;
         }
      }

      return min.iterator;
   }

   private ReverseSeekingIterator<InternalKey, Slice> getMax(){
      OrdinalIterator max = ordinalIterators.get(0);
      
      for(int i = 1; i < ordinalIterators.size(); i++){
         OrdinalIterator ord = ordinalIterators.get(i);
         if(ord.iterator.hasPrev() 
               && (!max.iterator.hasPrev() 
                     || largerPrev.compare(ord, max) < 0)){
            max = ord;
         }
      }

      return max.iterator;
   }

   @Override
   protected boolean hasNextInternal()
   {
      for(OrdinalIterator ord:ordinalIterators){
         if(ord.iterator.hasNext()){
            return true;
         }
      }
      return false;
   }

   @Override
   protected boolean hasPrevInternal()
   {
      for(OrdinalIterator ord:ordinalIterators){
         if(ord.iterator.hasPrev()){
            return true;
         }
      }
      return false;
   }

   @Override
   protected Entry<InternalKey, Slice> getNextElement()
   {
      return hasNextInternal() ? getMin().next() : null;
   }

   @Override
   protected Entry<InternalKey, Slice> getPrevElement()
   {
      return hasPrevInternal() ? getMax().prev() : null;
   }

   @Override
   protected Entry<InternalKey, Slice> peekInternal()
   {
      return hasNextInternal() ? getMin().peek() : null;
   }

   @Override
   protected Entry<InternalKey, Slice> peekPrevInternal()
   {
      return hasPrevInternal() ? getMax().peekPrev() : null;
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

   protected class SmallerNextElementComparator implements Comparator<OrdinalIterator> 
   {
      @Override
      public int compare(OrdinalIterator o1, OrdinalIterator o2)
      {
         if (o1.iterator.hasNext())
         {
            if (o2.iterator.hasNext())
            {
               //both iterators have a next element
               int result = comparator.compare(o1.iterator.peek().getKey(), o2.iterator.peek().getKey());
               return result == 0 ? Integer.compare(o1.ordinal, o2.ordinal) : result;
            }
            return -1; //o2 does not have a next element, consider o1 less than the empty o2
         }
         if(o2.iterator.hasNext()){
            return 1; //o1 does not have a next element, consider o2 less than the empty o1
         }
         return 0; //neither o1 nor o2 have a next element, consider them equals as empty iterators in this direction
      }
   }

   protected class LargerPrevElementComparator implements Comparator<OrdinalIterator>
   {
      @Override
      public int compare(OrdinalIterator o1, OrdinalIterator o2)
      {
         if (o1.iterator.hasPrev())
         {
            if (o2.iterator.hasPrev())
            {
               int result = comparator.compare(o1.iterator.peekPrev().getKey(), o2.iterator.peekPrev().getKey());
               return -(result == 0 ? Integer.compare(o1.ordinal, o2.ordinal) : result);
            }
            return 1; 
         }
         if(o2.iterator.hasPrev()){
            return -1; 
         }
         return 0;
      }
   }
}