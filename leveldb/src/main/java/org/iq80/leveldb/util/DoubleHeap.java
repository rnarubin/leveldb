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
// Date: Jul 2, 2014
// ---------------------

package org.iq80.leveldb.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class DoubleHeap<E>
{
   private static final int MIN_HEAP = 0, MAX_HEAP = 1;
   private final ArrayList<Heap<E>> heaps;
   private final Map<E, Node<E>> nodes;

   public DoubleHeap(Comparator<E> minComparator, Comparator<E> maxComparator)
   {
      nodes = new HashMap<>();

      heaps = new ArrayList<>(2);
      heaps.set(MIN_HEAP, new Heap<>(minComparator, MIN_HEAP));
      heaps.set(MAX_HEAP, new Heap<>(minComparator, MAX_HEAP));
   }

   public void add(E e)
   {
      Node<E> n = new Node<>(e);
      nodes.put(e, n);
      heaps.get(MIN_HEAP).add(n);
      heaps.get(MAX_HEAP).add(n);
   }
   
   public void addMin(E e){
      addSingle(MIN_HEAP, e);
   }

   public void addMax(E e){
      addSingle(MAX_HEAP, e);
   }
   
   private void addSingle(int whichHeap, E e){
      Node<E> n;
      if(nodes.containsKey(e)){
         n = nodes.get(e);
         if(n.index[whichHeap] != -1){
            //heap already contains this item
            return;
         }
         int other = other(whichHeap);
         if(n.index[other] != -1){
            heaps.get(other).siftDown(n.index[other]);
            heaps.get(other).siftUp(n.index[other]);
         }
      }
      else{
         n = new Node<E>(e);
      }
      heaps.get(whichHeap).add(n);
   }
   
   public E removeMin(){
      return removeSingle(MIN_HEAP);
   }
   
   public E removeMax(){
      return removeSingle(MAX_HEAP);
   }
   
   private E removeSingle(int whichHeap){
      Node<E> n = heaps.get(whichHeap).remove();
      
      if(n.index[other(whichHeap)] == -1){
         //node no longer exists in either heap
         nodes.remove(n.data);
      }
      
      return n.data;
   }
   
   public E peekMin(){
      return peekSingle(MIN_HEAP);
   }
   
   public E peekMax(){
      return peekSingle(MAX_HEAP);
   }
   
   private E peekSingle(int whichHeap){
      return heaps.get(whichHeap).peek().data;
   }
   
   private static int other(int whichHeap){
      return whichHeap^1;
   }
   
   private static class Heap<E>
   {
      private final Comparator<E> comparator;
      public ArrayList<Node<E>> arr;
      private final int selfIndex;

      public Heap(Comparator<E> comparator, final int selfIndex)
      {
         this.comparator = comparator;
         this.selfIndex = selfIndex;
      }

      public void add(Node<E> n)
      {
         arr.add(n);
         if (arr.size() > 1)
         {
            siftUp(arr.size() - 1, n);
         }
      }

      public Node<E> remove()
      {
         if (arr.size() == 0)
         {
            return null;
         }

         Node<E> last = arr.remove(arr.size() - 1);
         if (arr.size() == 0)
         {
            last.index[selfIndex] = -1;
            return last;
         }
         Node<E> ret = arr.get(0);
         siftDown(0, last);
         ret.index[selfIndex] = -1;

         return ret;
      }
      
      private Node<E> peek(){
         if(arr.size() == 0){
            return null;
         }
         return arr.get(0);
      }
      
      public void siftUp(int k){
         siftUp(k, arr.get(k));
      }
      
      public void siftDown(int k){
         siftDown(k, arr.get(k));
      }

      public void siftUp(int k, Node<E> n)
      {
         while (k > 0)
         {
            int parent = (k - 1) / 2;
            Node<E> p = arr.get(parent);
            if (comparator.compare(n.data, p.data) >= 0)
            {
               break;
            }
            p.setPosition(k, arr, selfIndex);
            k = parent;
         }
         n.setPosition(k, arr, selfIndex);
      }

      public void siftDown(int k, Node<E> n)
      {
         int half = arr.size() / 2;
         while (k < half)
         {
            int child = (k * 2) + 1;
            Node<E> c = arr.get(child);
            int right = child + 1;
            if (right < arr.size() && comparator.compare(c.data, arr.get(right).data) > 0)
            {
               child = right;
               c = arr.get(child);
            }
            if (comparator.compare(n.data, c.data) <= 0)
            {
               break;
            }
            c.setPosition(k, arr, selfIndex);
            k = child;
         }
         n.setPosition(k, arr, selfIndex);
      }

   }

   private static class Node<E>
   {
      public final E data;
      public int index[] = new int[2];

      public Node(E data)
      {
         this.data = data;
         index[MIN_HEAP] = -1;
         index[MAX_HEAP] = -1;
      }

      public void setPosition(int pos, ArrayList<Node<E>> arr, int whichHeap)
      {
         arr.set(pos, this);
         index[whichHeap] = pos;
      }
   }
}
