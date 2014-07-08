//
// Copyright (C) 2005-2011 Cleversafe, Inc. All rights reserved.
//
// Contact Information:
// Cleversafe, Inc.
// 222 South Riverside Plaza
// Suite 1700
// Chicago, IL 60606, USA
//
// licensing@cleversafe.com
//
// END-OF-HEADER
// -----------------------
// @author: renar
//
// Date: Jul 3, 2014
// ---------------------

package org.iq80.leveldb.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

import org.fusesource.leveldbjni.internal.NativeComparator.ComparatorJNI;
import org.iq80.leveldb.impl.ReverseIterators;
import org.iq80.leveldb.impl.ReversePeekingIterator;
import org.iq80.leveldb.impl.ReverseSeekingIterator;

import com.google.common.base.Function;

public class DoubleHeapIteratorTest extends TestCase
{
   private static final List<List<Integer>> lists;

   static
   {
      Random rand = new Random(0);
      lists = new ArrayList<>();

      for (int i = 0; i < 100; i++)
      { // 100 iterators to merge
         List<Integer> list = new ArrayList<>();
         for (int j = 0; j < rand.nextInt(9000) + 1000; j++)
         { // insert between 1000 and 10000 items into the list
            list.add(rand.nextInt());
         }
         Collections.sort(list); // lists must be sorted for merge (leveldb iterators operate on
// sorted data)
         lists.add(list);
      }
   }

   public void testForwardMergeIteration()
   {
      List<Integer> expected = new ArrayList<>();
      for (List<Integer> list : lists)
      {
         expected.addAll(list);
      }
      Collections.sort(expected);

      DoubleHeap<ReversePeekingIterator<Integer>> dh = populateDoubleHeap(getIterators(lists));

      List<Integer> actual = new ArrayList<>();
      while (dh.sizeMin() > 0)
      {
         ReversePeekingIterator<Integer> smallest = dh.removeMin();
         actual.add(smallest.next());
         if (smallest.hasNext())
         {
            dh.addMin(smallest);
         }
      }

      assertShallowListEquals(expected, actual);
   }

   public void testReverseMergeIteration()
   {
      List<Integer> expected = new ArrayList<>();
      for (List<Integer> list : lists)
      {
         expected.addAll(list);
      }
      Collections.sort(expected, Collections.reverseOrder());

      DoubleHeap<ReversePeekingIterator<Integer>> dh = new DoubleHeap<>(new
            SmallerNextComparator<Integer>(), new LargerPrevComparator<Integer>());
      List<ReversePeekingIterator<Integer>> iters = getIterators(lists);
      for (ReversePeekingIterator<Integer> iter : iters)
      {
         while (iter.hasNext())
         {
            iter.next();
         }
      }
      dh.addAll(iters);

      List<Integer> actual = new ArrayList<>();
      ReversePeekingIterator<Integer> largest;
      while ((largest = dh.removeMax()) != null)
      {
         if (largest.hasPrev())
         {
            actual.add(largest.prev());
            dh.addMax(largest);
         }
      }

      assertShallowListEquals(expected, actual);

   }

   public void testMixedMergeIteration()
   {
      List<Integer> sorted = new ArrayList<>();
      for (List<Integer> list : lists)
      {
         sorted.addAll(list);
      }
      Collections.sort(sorted);

      DoubleHeap<ReversePeekingIterator<Integer>> dh = populateDoubleHeap(getIterators(lists));

      // for mixing iteration forwards and backwards, we will take "steps" up and down the merged
// iterators
      // e.g. 4 steps forward, 2 steps back, 3 steps forward, 4 steps back
      // first walk up the list (favoring forward iteration in order to reach the end)
      // then walk back (favoring backwards iteration to reach the beginning)
      Random rand = new Random(0);
      List<Integer> steps = new ArrayList<>();
      int sum = 1;
      while (sum < sorted.size())
      {
         int step = rand.nextInt(12) - 4; // [-4, 7] inclusive, favoring forward
         if (step != 0)
         {
            steps.add(step);
            sum += step;
         }
      }
      sum = 0;
      while (sum >= 0)
      {
         int step = rand.nextInt(12) - 7; // [-7, 4] inclusive, favoring backward
         if (step != 0)
         {
            steps.add(step);
            sum += step;
         }
      }

      int expectedPos = 0;
      for (Integer step : steps)
      {
         int sign = step < 0 ? -1 : 1; // mathematical sign, used for addition
         for (int i = 0; Math.abs(i) < Math.abs(step) && expectedPos >= 0
               && expectedPos < sorted.size(); i += sign, expectedPos += sign)
         {
            Integer expected = sorted.get(expectedPos);
            Integer actual = null;
            if ((sign > 0 && expectedPos < sorted.size()) || (expectedPos == 0 && sign == -1)) // the
// zero element is a special case when moving backwards, move forwards instead
            {
               ReversePeekingIterator<Integer> smallest = dh.removeMin();
               actual = smallest.next();
               if (smallest.hasNext())
               {
                  dh.addMin(smallest);
               }
            }
            else if (expectedPos >= 1)
            {
               ReversePeekingIterator<Integer> largest = dh.removeMax();
               actual = largest.prev();
               if (largest.hasPrev())
               {
                  dh.addMax(largest);
               }
            }
            assertEquals("Item #" + expectedPos + " mismatch", expected, actual);
         }
      }
   }

   private <T extends Comparable<T>> DoubleHeap<ReversePeekingIterator<T>> populateDoubleHeap(
         Collection<ReversePeekingIterator<T>> c)
   {
      DoubleHeap<ReversePeekingIterator<T>> dh =
            new DoubleHeap<>(new SmallerNextComparator<T>(),
                  new LargerPrevComparator<T>());
      dh.addAll(c);
      return dh;
   }

   private <T> List<ReversePeekingIterator<T>> getIterators(Collection<List<T>> lists)
   {
      List<ReversePeekingIterator<T>> ret = new ArrayList<>();
      for (List<T> list : lists)
      {
         ret.add(ReverseIterators.reversePeekingIterator(list));
      }
      return ret;
   }

   private <T> void assertShallowListEquals(List<T> expected, List<T> actual)
   {
      assertEquals("List size mismatch", expected.size(), actual.size());
      Iterator<T> bIter = actual.iterator();
      int i = 0;
      for (T t : expected)
      {
         assertEquals("Item #" + (i++) + " mismatch", t, bIter.next());
      }
   }

   private static class SmallerNextComparator<T extends Comparable<T>>
         implements
         Comparator<ReversePeekingIterator<T>>
   {
      @Override
      public int compare(ReversePeekingIterator<T> o1, ReversePeekingIterator<T> o2)
      {
         if (o1.hasNext())
         {
            if (o2.hasNext())
            {
               return o1.peek().compareTo(o2.peek());
            }
            return -1; // o2 does not have a next, o1 is "smaller" than empty
         }
         if (o2.hasNext())
         {
            return 1; // o1 does not have a next
         }
         return 0; // neither have a next
      }
   }

   private static class LargerPrevComparator<T extends Comparable<T>>
         implements
         Comparator<ReversePeekingIterator<T>>
   {
      private int preReverseCompare(ReversePeekingIterator<T> o1, ReversePeekingIterator<T> o2)
      {
         if (o1.hasPrev())
         {
            if (o2.hasPrev())
            {
               return o1.peekPrev().compareTo(o2.peekPrev());
            }
            return -1;
         }
         if (o2.hasPrev())
         {
            return 1;
         }
         return 0;
      }

      @Override
      public int compare(ReversePeekingIterator<T> o1, ReversePeekingIterator<T> o2)
      {
         return -preReverseCompare(o1, o2);
      }
   }
}
