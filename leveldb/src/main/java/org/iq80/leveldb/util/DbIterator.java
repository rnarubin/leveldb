
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

import static org.iq80.leveldb.util.DbIterator.ValidQuickCheck.*;

public final class DbIterator extends AbstractReverseSeekingIterator<InternalKey, Slice>
      implements
         InternalIterator
{

   private final ArrayList<OrdinalIterator> ordinalIterators;
   private final ElementComparator smallerNext, largerPrev;
   private ValidQuickCheck quickCheck = NONE;

   private final Comparator<InternalKey> comparator;

   //private final DoubleHeap<OrdinalIterator> doubleHeap;
   protected enum ValidQuickCheck{
      //indicates whether the ordinal iterators list can be checked at index 0 or size-1 for the min or max item respectively
      MIN, MAX, NONE
   }

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

      //this.doubleHeap = new DoubleHeap<>(new SmallerNextElementComparator(), new LargerPrevElementComparator());
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
   
   private ReverseSeekingIterator<InternalKey, Slice> extremeToEnd(Function<ReverseSeekingIterator<InternalKey, Slice>, Boolean> hasFollowing, ElementComparator elemCompare, final ValidQuickCheck quickCheckDirection, final int endIndex){
      /*
       * in the DoubleHeap approach it proved difficult to coordinate the two heaps when reverse iterations is necessary
       * instead, just perform linear search of the iterators for the min or max item
       * there tends to be only a small number of iterators (less than 10) even when the database contains a substantial number of items (in the millions)
       */
      int extremeIndex = endIndex; //minIndex = 0 or maxIndex = list.size()-1
      OrdinalIterator extreme = ordinalIterators.get(extremeIndex);

      if(quickCheck != quickCheckDirection){ //quickCheck != MIN or != MAX
         for(int i = 0; i < ordinalIterators.size(); i++){
            OrdinalIterator following = ordinalIterators.get(i);
            if(hasFollowing.apply(following.iterator) && (!hasFollowing.apply(extreme.iterator) || elemCompare.compare(following, extreme) < 0)){
               //if the following iterator hasNext (or hasPrev, larger, etc.) and the current minimum does not
               //or if the following is simply smaller
               //we've found a new minimum
               extremeIndex = i;
               extreme = following;
            }
         }
         Collections.swap(ordinalIterators, extremeIndex, endIndex); //move the min to 0 or max to list.size()-1
         quickCheck = quickCheckDirection; //keep record of this for quick lookup
      }

      return extreme.iterator;
      
   }

   private ReverseSeekingIterator<InternalKey, Slice> minToFront(){
      return extremeToEnd(ReverseIterators.<ReverseSeekingIterator<InternalKey, Slice>>hasNext(), smallerNext, MIN, 0);
   }

   private ReverseSeekingIterator<InternalKey, Slice> maxToBack(){
      return extremeToEnd(ReverseIterators.<ReverseSeekingIterator<InternalKey, Slice>>hasPrev(), largerPrev, MAX, ordinalIterators.size()-1);
   }

   @Override
   protected boolean hasNextInternal()
   {
      return (quickCheck == MIN && ordinalIterators.get(0).iterator.hasNext()) || hasMin();
   }

   @Override
   protected boolean hasPrevInternal()
   {
      return (quickCheck == MAX && ordinalIterators.get(ordinalIterators.size()-1).iterator.hasPrev()) || hasMin();
   }

   @Override
   protected Entry<InternalKey, Slice> getPrevElement()
   {
      if(!hasPrevInternal()){
         return null;
      }

      Entry<InternalKey, Slice> max = maxToBack().prev();
      quickCheck = NONE;
      
      return max;
      /*
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
      //if(!hadNext){
      if(largest.iterator.hasNext()){
         doubleHeap.addMin(largest);
      }

      return result;
      */
   }

   @Override
   protected Entry<InternalKey, Slice> getNextElement()
   {
      if(!hasNextInternal()){
         return null;
      }
      
      Entry<InternalKey, Slice> min = minToFront().next();
      quickCheck = NONE; //min just advanced, quick check isn't valid to check the min

      return min;
      /*
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
      //if(!hadPrev){
      if(smallest.iterator.hasPrev()){
         doubleHeap.addMax(smallest);
      }

      return result;
      */
   }

   private void resetPriorityQueue()
   {
      quickCheck = NONE;
      //doubleHeap.clear();
      /*
      for(OrdinalIterator ord:ordinalIterators){
         if(ord.iterator.hasNext()){
            doubleHeap.addMin(ord);
         }
         if(ord.iterator.hasPrev()){
            doubleHeap.addMax(ord);
         }
      }
      */
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