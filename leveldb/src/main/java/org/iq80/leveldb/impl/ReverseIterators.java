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
// @author: rnarubin
//
// Date: Jun 30, 2014
// ---------------------

package org.iq80.leveldb.impl;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.testng.collections.Lists;

import com.google.common.collect.PeekingIterator;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public final class ReverseIterators<E>
{

   // reimplements several methods and classes from com.google.common.collect.Iterators
   // in addition to further reversing functionality and convenience methods
   // in order to accommodate reverse iteration
    
   public static <T> ListReverseIterator<T> listReverseIterator(ListIterator<T> listIter){
      return new ListReverseIterator<T>(listIter);
   }
     
   public static <T> ListReverseIterator<T> listReverseIterator(List<T> list){
      return listReverseIterator(list.listIterator());
   }
     
   public static <T> ListReverseIterator<T> listReverseIterator(Collection<T> collection){
      return listReverseIterator(Lists.newArrayList(collection));
   }
   
   public static <T> ReversePeekingIterator<T> reversePeekingIterator(
         ListIterator<? extends T> listIter)
   {
      return reversePeekingIterator(ReverseIterators.listReverseIterator(listIter));
   }
   
   public static <T> ReversePeekingIterator<T> reversePeekingIterator(
         List<? extends T> list)
   {
      return reversePeekingIterator(ReverseIterators.listReverseIterator(list));
   }
   
   public static <T> ReversePeekingIterator<T> reversePeekingIterator(
         Collection<? extends T> collection)
   {
      return reversePeekingIterator(ReverseIterators.listReverseIterator(collection));
   }
   
   public static <T> ReversePeekingIterator<T> reversePeekingIterator(
         ReverseIterator<? extends T> iterator)
   {
      if (iterator instanceof ReversePeekingImpl)
      {
         @SuppressWarnings("unchecked")
         ReversePeekingImpl<T> rPeeking = (ReversePeekingImpl<T>) iterator;
         return rPeeking;
      }
      return new ReversePeekingImpl<T>(iterator);
   }
   
   private static class ListReverseIterator<E> implements ReverseIterator<E>{
      private final ListIterator<E> iter;
      
      public ListReverseIterator(ListIterator<E> listIterator){
         this.iter = listIterator;
      }
      @Override
      public boolean hasNext()
      {
         return iter.hasNext();
      }

      @Override
      public E next()
      {
         return iter.next();
      }

      @Override
      public void remove()
      {
         iter.remove();
      }

      @Override
      public E prev()
      {
         return iter.previous();
      }

      @Override
      public boolean hasPrev()
      {
         return iter.hasPrevious();
      }
      
   }

   private static class ReversePeekingImpl<E> extends PeekingImpl<E>
         implements
            ReversePeekingIterator<E>
   {
      private final ReverseIterator<? extends E> rIterator;
      private boolean rHasPeeked;
      private E rPeekedElement;

      public ReversePeekingImpl(ReverseIterator<? extends E> iterator)
      {
         super(iterator);
         this.rIterator = iterator;
      }

      @Override
      public void remove()
      {
         checkState(!rHasPeeked, "Can't remove after you've peeked at previous");
         super.remove();
      }

      @Override
      public boolean hasPrev()
      {
         return rHasPeeked || rIterator.hasPrev();
      }

      @Override
      public E prev()
      {
         if (!rHasPeeked)
         {
            return rIterator.prev();
         }
         E result = rPeekedElement;
         rHasPeeked = false;
         rPeekedElement = null;
         return result;
      }

      @Override
      public E peekPrev()
      {
         if (!rHasPeeked)
         {
            rPeekedElement = rIterator.prev();
            rHasPeeked = true;
         }
         return rPeekedElement;
      }

   }

   /*
    * Copyright (C) 2007 The Guava Authors
    * 
    * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
    * except in compliance with the License. You may obtain a copy of the License at
    * 
    * http://www.apache.org/licenses/LICENSE-2.0
    * 
    * Unless required by applicable law or agreed to in writing, software distributed under the
    * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
    * either express or implied. See the License for the specific language governing permissions and
    * limitations under the License.
    */

   private static class PeekingImpl<E> implements PeekingIterator<E>
   {

      private final Iterator<? extends E> iterator;
      private boolean hasPeeked;
      private E peekedElement;

      public PeekingImpl(Iterator<? extends E> iterator)
      {
         this.iterator = checkNotNull(iterator);
      }

      @Override
      public boolean hasNext()
      {
         return hasPeeked || iterator.hasNext();
      }

      @Override
      public E next()
      {
         if (!hasPeeked)
         {
            return iterator.next();
         }
         E result = peekedElement;
         hasPeeked = false;
         peekedElement = null;
         return result;
      }

      @Override
      public void remove()
      {
         checkState(!hasPeeked, "Can't remove after you've peeked at next");
         iterator.remove();
      }

      @Override
      public E peek()
      {
         if (!hasPeeked)
         {
            peekedElement = iterator.next();
            hasPeeked = true;
         }
         return peekedElement;
      }
   }
   
   /*
    * end copyright
    */

}
