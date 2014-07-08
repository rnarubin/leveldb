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
// Date: Jul 8, 2014
//---------------------

package org.iq80.leveldb.util;
import java.util.Arrays;

import org.iq80.leveldb.impl.ReverseIterators;
import org.iq80.leveldb.impl.ReversePeekingIterator;

import junit.framework.TestCase;

public class ReversePeekingIteratorTest extends TestCase
{
   public void testNextPrevPeekPeekPrev(){
      Integer a = 0, b = 1, c = 2, d = 3;
      ReversePeekingIterator<Integer> iter = ReverseIterators.reversePeekingIterator(Arrays.asList(a, b, c, d));
      
      assertTrue(iter.hasNext());
      assertFalse(iter.hasPrev());
      assertEquals(a, iter.peek());
      
      //make sure the peek did not advance anything
      assertTrue(iter.hasNext());
      assertFalse(iter.hasPrev());
      assertEquals(a, iter.peek());

      assertEquals(a, iter.next());
      assertTrue(iter.hasNext());
      assertTrue(iter.hasPrev());
      assertEquals(a, iter.peekPrev());

      assertTrue(iter.hasNext());
      assertTrue(iter.hasPrev());
      assertEquals(b, iter.peek());
      assertEquals(b, iter.next());

      assertTrue(iter.hasNext());
      assertTrue(iter.hasPrev());
      assertEquals(b, iter.peekPrev());
      assertEquals(c, iter.peek());
      
      assertEquals(b, iter.prev());
      assertEquals(b, iter.peek());
      assertEquals(a, iter.peekPrev());

      assertEquals(b, iter.next());
      assertEquals(c, iter.next());
      assertEquals(d, iter.next());
      assertFalse(iter.hasNext());
      assertTrue(iter.hasPrev());
   }

}


