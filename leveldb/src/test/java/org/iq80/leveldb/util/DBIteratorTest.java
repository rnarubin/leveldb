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
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import junit.framework.TestCase;
import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;

import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.impl.ReverseIterator;
import org.iq80.leveldb.impl.ReverseIterators;
import org.iq80.leveldb.impl.ReverseSeekingIterator;

import com.google.common.base.Function;

public class DBIteratorTest extends TestCase
{
   private static final byte[] value = {0,0,0,0};
   private static final List<Slice> keys;
   private final Options options = new Options().createIfMissing(true);
   private DB db;
   private File tempDir;
   
   static{
      Random rand = new Random(0);
      
      keys = new ArrayList<>();
      int items = 100_000;
      for(int i = 0; i < items; i++){
         byte[] b = new byte[128];
         rand.nextBytes(b);
         keys.add(Slices.wrappedBuffer(b)); // for sorting and equality purposes, use the same internal structure as the DB uses
      }
   }
   
   @Override
   protected void setUp() throws Exception
   {
      tempDir = FileUtils.createTempDir("java-leveldb-testing-temp");
      db = factory.open(tempDir, options);
   }
   
   @Override
   protected void tearDown() throws Exception
   {
      try{
         db.close();
      }
      finally{
         FileUtils.deleteRecursively(tempDir);
      }
   }
   
   public void testForwardIteration(){
      for(Slice k:keys){
         db.put(k.getBytes(), value);
      }

      Collections.sort(keys);
      
      DBIterator actual = db.iterator();
      Iterator<Slice> expected = keys.iterator();
      for(int i = 0; expected.hasNext(); i++){
         assertTrue(actual.hasNext());
         assertEquals("Item #"+i+" mismatch", expected.next(), Slices.wrappedBuffer(actual.next().getKey()));
      }
   }
   
   public void testReverseIteration(){
      for(Slice k:keys){
         db.put(k.getBytes(), value);
      }

      Collections.sort(keys, Collections.reverseOrder()); //reverse sort to simulate reverse traversal
      
      DBIterator actual = db.iterator();
      actual.seekToLast();
      Iterator<Slice> expected = keys.iterator();
      for(int i = keys.size()-1; expected.hasNext(); i--){
         assertTrue(actual.hasPrev());
         assertEquals("Item #"+i+" mismatch", expected.next(), Slices.wrappedBuffer(actual.prev().getKey()));
      }
   }
   
   public void testMixedIteration(){
      Random rand = new Random(0);
      
      for(Slice k:keys){
         db.put(k.getBytes(), value);
      }
      
      Collections.sort(keys);
      
      ReverseIterator<Slice> expected = ReverseIterators.listReverseIterator(keys);
      ReverseSeekingIterator<byte[], byte[]> actual = ReverseIterators.wrap(db.iterator()); //a simple wrapper to make the functional style below work properly
      
      List<Function<ReverseIterator<?>, Boolean>> hasFollowing = new ArrayList<>();
      hasFollowing.add(new Function<ReverseIterator<?>, Boolean>(){ 
         // index 0 implies direction backwards, check previous
         public Boolean apply(ReverseIterator<?> iter){
            return iter.hasPrev();
         }
      });
      hasFollowing.add(new Function<ReverseIterator<?>, Boolean>(){ 
         // index 1 implies direction forwards, check next 
         public Boolean apply(ReverseIterator<?> iter){
            return iter.hasNext();
         }
      });

      List<Function<ReverseIterator<Slice>, Slice>> getFollowingExpected = new ArrayList<>();
      getFollowingExpected.add(makePrevGetter(expected));
      getFollowingExpected.add(makeNextGetter(expected));

      List<Function<ReverseIterator<Entry<byte[], byte[]>>, Entry<byte[], byte[]>>> getFollowingActual = new ArrayList<>();
      getFollowingActual.add(makePrevGetter(actual));
      getFollowingActual.add(makeNextGetter(actual));
      
      //take mixed forward and backward steps up the list then down the list (favoring forward to reach the end, then backward)
      int pos = 0;
      int randForward = 12, randBack = 4;// [-4, 7] inclusive, initially favor forward steps
      int steps = randForward+1;
      do{
         int sign = steps<0?-1:1; // mathematical sign for addition
         int direction = steps<0?0:1; //logical direction for indexing
         for(int i = 0; Math.abs(i) < Math.abs(steps); i+=sign){
            //if the expected iterator has items in this direction, proceed
            if(hasFollowing.get(direction).apply(expected)){
               assertTrue(hasFollowing.get(direction).apply(actual));

               //fancy way of asserting expected.next() equals actual.next() or expected.prev() equals actual.prev() given the direction
               assertEquals("Item #"+pos+" mismatch", getFollowingExpected.get(direction).apply(expected),
                     Slices.wrappedBuffer(getFollowingActual.get(direction).apply(actual).getKey()));
               //in hind sight, writing steps<0?actual.prev():actual.next() would have been simpler, albeit repetitive
               //but i've already written all this code, and higher order functions are cool, right?
               //I should come back in java 8 and fix this with real lambdas
               
               pos += sign;
            }
            else break;
         }
         if(pos >= keys.size()){
            //switch to favor backward steps
            randForward = 4;
            randBack = 12;
            //[-7, 4] inclusive
         }
         steps = rand.nextInt(randForward)-randBack;
      }while(pos > 0);
   }
   
   public <T> Function<ReverseIterator<T>, T> makeNextGetter(ReverseIterator<T> iter){
      return new Function<ReverseIterator<T>, T>(){ 
         public T apply(ReverseIterator<T> iter){
            return iter.next();
         }
      };
   }
   
   public <T> Function<ReverseIterator<T>, T> makePrevGetter(ReverseIterator<T> iter){
      return new Function<ReverseIterator<T>, T>(){ 
         public T apply(ReverseIterator<T> iter){
            return iter.prev();
         }
      };
   }
}