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
//-----------------------
// @author: renar
//
// Date: Jul 3, 2014
//---------------------

package org.iq80.leveldb.util;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang3.tuple.Pair;

import junit.framework.TestCase;

public class DoubleHeapTest extends TestCase
{
   
   private static final List<Pair<Integer, Integer>> nums;
   
   static{
      int size = 1000;
      Random rand = new Random(0);
      nums = new ArrayList<>(size);
      for(int i = 0; i < size; i++){
         nums.add(Pair.of(i, rand.nextInt()));
      }
      Collections.shuffle(nums, rand);
   }
   
   public void testSequentialAddRemove(){
      DoubleHeap<Pair<Integer, Integer>> dh = populateDH(nums);
      
      List<Integer> dhMins = new ArrayList<Integer>();
      
      while(dh.sizeMin() > 0){
         dhMins.add(dh.removeMin().getLeft());
      }
      
      assertShallowListEquals(sortLeft(nums), dhMins);
      
      List<Integer> dhMaxs = new ArrayList<Integer>();
      while(dh.sizeMax() > 0){
         dhMaxs.add(dh.removeMax().getRight());
      }
      
      assertShallowListEquals(sortRight(nums), dhMaxs);
   }
   
   public void testMixedAddRemove(){
      Random rand = new Random(0);

      DoubleHeap<Pair<Integer, Integer>> dh = new DoubleHeap<>(new LeftComparator<Integer, Integer>(), new RightComparator<Integer, Integer>());
      List<Pair<Integer, Integer>> toInsert = new ArrayList<>(nums);
      List<Integer> insertedMin = new ArrayList<>(), insertedMax = new ArrayList<>();
      Collections.shuffle(toInsert, rand);

      List<Integer> ranges = new ArrayList<>();
      for(int i  = 0; i < toInsert.size(); i+=rand.nextInt(15)+1){
         ranges.add(i);
      }
      if(ranges.get(ranges.size()-1) != toInsert.size()){
         ranges.set(ranges.size()-1, toInsert.size());
      }
      
      for(int i = 1; i < ranges.size(); i++){
         for(Pair<Integer, Integer> p:toInsert.subList(ranges.get(i-1), ranges.get(i))){
            dh.add(p);
            insertedMin.add(p.getLeft());
            insertedMax.add(p.getRight());
         }
         int numToRemove = Math.min(rand.nextInt(15), insertedMin.size());
         Collections.sort(insertedMin);
         List<Integer> expectedMin = insertedMin.subList(0, numToRemove),
               returnedMin = new ArrayList<>();
               
         for(int j = 0; j < numToRemove; j++){
            returnedMin.add(dh.removeMin().getLeft());
         }
         
         assertShallowListEquals(expectedMin, returnedMin);
         
         expectedMin.clear();

         numToRemove = Math.min(rand.nextInt(15), insertedMax.size());
         Collections.sort(insertedMax);
         List<Integer> expectedMax = insertedMax.subList(0, numToRemove),
               returnedMax = new ArrayList<>();
               
         for(int j = 0; j < numToRemove; j++){
            returnedMax.add(dh.removeMax().getRight());
         }
         
         assertShallowListEquals(expectedMax, returnedMax);
         
         expectedMax.clear();
      }
      
   }
   
   private <K extends Comparable<K>, V extends Comparable<V>> DoubleHeap<Pair<K, V>> populateDH(Collection<Pair<K, V>> c){
      DoubleHeap<Pair<K, V>> dh =
            new DoubleHeap<>(new LeftComparator<K, V>(),
                  new RightComparator<K, V>());

      for(Pair<K, V> pair:c){
         dh.add(pair);
      }
      
      return dh;
   }
   
   private <K extends Comparable<K>, V> List<K> sortLeft(Collection<Pair<K, V>> c){
      List<K> ret = new ArrayList<>();
      for(Pair<K, V> p:c){
         ret.add(p.getLeft());
      }
      Collections.sort(ret);
      return ret;
   }
   
   private <K, V extends Comparable<V>> List<V> sortRight(Collection<Pair<K, V>> c){
      List<V> ret = new ArrayList<>();
      for(Pair<K, V> p:c){
         ret.add(p.getRight());
      }
      Collections.sort(ret);
      return ret;
   }
   
   private <T> void assertShallowListEquals(List<T> a, List<T> b){
      assertTrue(a.size()==b.size());
      boolean sameItems = true;
      Iterator<T> bIter = b.iterator();
      for(T t:a){
         if(!(sameItems = t.equals(bIter.next()))){
            break;
         }
      }
      assertTrue(sameItems);
   }
   
   
   private static class LeftComparator<K extends Comparable<K>, V> implements Comparator<Pair<K, V>>{
      @Override
      public int compare(Pair<K, V> o1, Pair<K, V> o2)
      {
         return o1.getLeft().compareTo(o2.getLeft());
      }
   }
   
   private static class RightComparator<K, V extends Comparable<V>> implements Comparator<Pair<K, V>>{
      @Override
      public int compare(Pair<K, V> o1, Pair<K, V> o2)
      {
         return o1.getRight().compareTo(o2.getRight());
      }
   }

}


