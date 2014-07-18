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
// Date: Jul 8, 2014
// ---------------------

package org.iq80.leveldb.util;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import junit.framework.TestCase;
import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;

import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.impl.ReverseIterator;
import org.iq80.leveldb.impl.ReverseIterators;
import org.iq80.leveldb.impl.ReversePeekingIterator;
import org.iq80.leveldb.impl.ReverseSeekingIterator;

import com.google.common.collect.Maps;

import static com.google.common.base.Charsets.UTF_8;

public class DBIteratorTest extends TestCase
{
   private static final List<Entry<String, String>> entries;
   private final Options options = new Options().createIfMissing(true);
   private DB db;
   private File tempDir;

   static
   {
      Random rand = new Random(0);

      entries = new ArrayList<>();
      int items = 100_000;
      for (int i = 0; i < items; i++)
      {
         StringBuilder sb = new StringBuilder();
         for (int j = 0; j < 20; j++)
         {
            sb.append((char) ('a' + rand.nextInt(26)));
         }
         entries.add(Maps.immutableEntry(sb.toString(), "v:" + sb.toString()));
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
      try
      {
         db.close();
      }
      finally
      {
         FileUtils.deleteRecursively(tempDir);
      }
   }

   public void testForwardIteration()
   {
      for (Entry<String, String> e : entries)
      {
         db.put(e.getKey().getBytes(), e.getValue().getBytes());
      }

      Collections.sort(entries, new StringDbIterator.EntryCompare());

      StringDbIterator actual = new StringDbIterator(db.iterator());
      int i = 0;
      for(Entry<String, String> expected:entries)
      {
         assertTrue(actual.hasNext());
         Entry<String, String> p = actual.peek();
         assertEquals("Item #" + i + " peek mismatch", expected, p);
         Entry<String, String> n = actual.next();
         assertEquals("Item #" + i + " mismatch", expected, n);
         i++;
      }
   }

   public void testReverseIteration()
   {
      for (Entry<String, String> e : entries)
      {
         db.put(e.getKey().getBytes(), e.getValue().getBytes());
      }

      Collections.sort(entries, new StringDbIterator.ReverseEntryCompare());

      StringDbIterator actual = new StringDbIterator(db.iterator());
      actual.seekToLast();
      int i = 0;
      for(Entry<String, String> expected:entries)
      {
         assertTrue(actual.hasPrev());
         assertEquals("Item #" + i + " peek mismatch", expected, actual.peekPrev());
         assertEquals("Item #" + i + " mismatch", expected, actual.prev());
         i++;
      }
   }

   public void testMixedIteration()
   {
      Random rand = new Random(0);

      for (Entry<String, String> e : entries)
      {
         db.put(e.getKey().getBytes(), e.getValue().getBytes());
      }

      Collections.sort(entries, new StringDbIterator.EntryCompare());

      ReversePeekingIterator<Entry<String, String>> expected =
            ReverseIterators.reversePeekingIterator(entries);
      ReverseSeekingIterator<String, String> actual = new StringDbIterator(db.iterator());

      // take mixed forward and backward steps up the list then down the list (favoring forward to reach the end, then backward)
      int pos = 0;
      int randForward = 12, randBack = 4;// [-4, 7] inclusive, initially favor forward steps
      int steps = randForward + 1;
      do
      {
         int direction = steps < 0 ? -1 : 1; // mathematical sign for addition
         for (int i = 0; Math.abs(i) < Math.abs(steps); i += direction)
         {
            // if the expected iterator has items in this direction, proceed
            if (hasFollowing(direction, expected))
            {
               assertTrue(hasFollowing(direction, actual));

               assertEquals("Item #" + pos + " mismatch",
                     peekFollowing(direction, expected),
                     peekFollowing(direction, actual));

               assertEquals("Item #" + pos + " mismatch",
                     getFollowing(direction, expected),
                     getFollowing(direction, actual));

               pos += direction;
            }
            else
               break;
         }
         if (pos >= entries.size())
         {
            // switch to favor backward steps
            randForward = 4;
            randBack = 12;
            // [-7, 4] inclusive
         }
         steps = rand.nextInt(randForward) - randBack;
      } while (pos > 0);
   }
   
   public boolean hasFollowing(int direction, ReverseIterator<?> iter){
      return direction<0?iter.hasPrev():iter.hasNext();
   }
   
   public <T> T peekFollowing(int direction, ReversePeekingIterator<T> iter){
      return direction<0?iter.peekPrev():iter.peek();
   }
   
   public <T> T getFollowing(int direction, ReverseIterator<T> iter){
      return direction<0?iter.prev():iter.next();
   }

   private static class StringDbIterator implements ReverseSeekingIterator<String, String>
   {
      private DBIterator iterator;

      private StringDbIterator(DBIterator iterator)
      {
         this.iterator = iterator;
      }

      @Override
      public boolean hasNext()
      {
         return iterator.hasNext();
      }

      @Override
      public void seekToFirst()
      {
         iterator.seekToFirst();
      }

      @Override
      public void seek(String targetKey)
      {
         iterator.seek(targetKey.getBytes(UTF_8));
      }

      @Override
      public Entry<String, String> peek()
      {
         return adapt(iterator.peekNext());
      }

      @Override
      public Entry<String, String> next()
      {
         return adapt(iterator.next());
      }

      @Override
      public void remove()
      {
         throw new UnsupportedOperationException();
      }

      private Entry<String, String> adapt(Entry<byte[], byte[]> next)
      {
         return Maps.immutableEntry(new String(next.getKey(), UTF_8), new String(next.getValue(),
               UTF_8));
      }

      @Override
      public Entry<String, String> peekPrev()
      {
         return adapt(iterator.peekPrev());
      }

      @Override
      public Entry<String, String> prev()
      {
         return adapt(iterator.prev());
      }

      @Override
      public boolean hasPrev()
      {
         return iterator.hasPrev();
      }

      @Override
      public void seekToLast()
      {
         iterator.seekToLast();
      }

      public static class EntryCompare implements Comparator<Entry<String, String>>
      {
         @Override
         public int compare(Entry<String, String> o1, Entry<String, String> o2)
         {
            return o1.getKey().compareTo(o2.getKey());
         }
      }
      public static class ReverseEntryCompare extends EntryCompare
      {
         @Override
         public int compare(Entry<String, String> o1, Entry<String, String> o2)
         {
            return -super.compare(o1, o2);
         }
      }
   }
}
