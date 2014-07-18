
package org.iq80.leveldb.util;

import com.google.common.base.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.ReverseIterators;
import org.iq80.leveldb.impl.MemTable.MemTableIterator;
import org.iq80.leveldb.impl.ReverseSeekingIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;

public final class DbIterator extends AbstractReverseSeekingIterator<InternalKey, Slice>
      implements
         InternalIterator
{

   private final ArrayList<OrdinalIterator> ordinalIterators;
   private final ElementComparator smallerNext, largerPrev;

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
         ord.iterator.seekToLast();
      }
   }

   @Override
   protected void seekInternal(InternalKey targetKey)
   {
      for(OrdinalIterator ord:ordinalIterators){
         ord.iterator.seek(targetKey);
      }
   }
   
   private boolean hasExtreme(Function<ReverseSeekingIterator<InternalKey, Slice>, Boolean> hasFollowing){ 
      for(OrdinalIterator ord:ordinalIterators){
         if(hasFollowing.apply(ord.iterator)){
            return true;
         }
      }
      return false;
   }

   private boolean hasMin(){
      return hasExtreme(ReverseIterators.<ReverseSeekingIterator<InternalKey, Slice>>hasNext());
   }

   private boolean hasMax(){
      return hasExtreme(ReverseIterators.<ReverseSeekingIterator<InternalKey, Slice>>hasPrev());
   }
   
   private ReverseSeekingIterator<InternalKey, Slice> getExtreme(Function<ReverseSeekingIterator<InternalKey, Slice>, Boolean> hasFollowing, ElementComparator elemCompare){
      /*
       * in the DoubleHeap approach it proved difficult to coordinate the two heaps when reverse iterations is necessary
       * instead, just perform linear search of the iterators for the min or max item
       * there tends to be only a small number of iterators (less than 10) even when the database contains a substantial number of items (in the millions)
       */
      OrdinalIterator extreme = ordinalIterators.get(0);

      for(int i = 1; i < ordinalIterators.size(); i++){
         OrdinalIterator ord = ordinalIterators.get(i);
         if(hasFollowing.apply(ord.iterator) && (!hasFollowing.apply(extreme.iterator) || elemCompare.compare(ord, extreme) < 0)){
            //if the following iterator hasNext (or hasPrev, larger, etc.) and the current minimum does not
            //or if the following is simply smaller
            //we've found a new minimum
            extreme = ord;
         }
      }

      return extreme.iterator;
      
   }

   private ReverseSeekingIterator<InternalKey, Slice> getMin(){
      return getExtreme(ReverseIterators.<ReverseSeekingIterator<InternalKey, Slice>>hasNext(), smallerNext);
   }

   private ReverseSeekingIterator<InternalKey, Slice> getMax(){
      return getExtreme(ReverseIterators.<ReverseSeekingIterator<InternalKey, Slice>>hasPrev(), largerPrev);
   }

   @Override
   protected boolean hasNextInternal()
   {
      return hasMin();
   }

   @Override
   protected boolean hasPrevInternal()
   {
      return hasMax();
   }

   @Override
   protected Entry<InternalKey, Slice> getPrevElement()
   {
      if(!hasMax()){
         return null;
      }

      return getMax().prev();
   }

   @Override
   protected Entry<InternalKey, Slice> getNextElement()
   {
      if(!hasMin()){
         return null;
      }
      
      return getMin().next();
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