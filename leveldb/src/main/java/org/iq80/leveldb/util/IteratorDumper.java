
package org.iq80.leveldb.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.FileMetaData;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.Iq80DBFactory;
import org.iq80.leveldb.impl.SeekingIteratorAdapter;
import org.iq80.leveldb.impl.TableCache;

public class IteratorDumper
{
   public static void main(String[] args) throws IOException{
      DB db = Iq80DBFactory.factory.open(FileUtils.createTempDir("leveldb"), new Options().useMMap(false));
      DBIterator iter = db.iterator();
      
      //System.out.println(dump(db, iter));
      System.out.println(prettyFormat("[a=123,b=[c=456,d=789],q=qwer,p=[],z={},d=[1,2,3,4],e={1,23,43,53}]"));
   }
   public static String dump(DB db, DBIterator DBiter){
      StringBuilder sb = new StringBuilder("DBIterator dump:\n");
      if(!(db instanceof org.iq80.leveldb.impl.DbImpl) || !(DBiter instanceof org.iq80.leveldb.impl.SeekingIteratorAdapter))
      {
         sb.append("Not an Iq80LevelDB iterator");
         return sb.toString();
      }
      
      append(sb, getField(db, "options"));

      SeekingIteratorAdapter iter = (SeekingIteratorAdapter)DBiter;
      append(sb, iter);
      
      return sb.toString();
   }
   
   private static Object getField(Object owner, String varName)
   {
      try
      {
         return FieldUtils.readDeclaredField(owner, varName, true);
      }
      catch (IllegalAccessException e){}
      return null;
   }

   private static Object getSubField(Object owner, String vars){
      for(String var:vars.split("\\."))
      {
         owner = getField(owner, var);
      }
      return owner;
   }

   @SuppressWarnings("serial")
   private static final ToStringStyle toStringStyle = new RecursiveToStringStyle(){
      {
         this.setArrayStart("[");
         this.setArrayEnd("]");
         this.setContentStart("{");
         this.setContentEnd("}");
      }
      HashSet<Class<?>> skipClasses = new HashSet<Class<?>>(Arrays.asList(Slice.class, TableCache.class, FileMetaData.class, InternalKey.class));
      protected boolean accept(Class<?> clazz) {
         return !skipClasses.contains(clazz);
      };
   };
   private static void append(StringBuilder sb, Object o){
      sb.append(ReflectionToStringBuilder.toString(o, toStringStyle)).append("\n");
   }

   public static String prettyFormat(String s)
   {
      StringBuilder sb = new StringBuilder();
      StringBuilder tabs = new StringBuilder();
      for(int i = 0; i < s.length(); i++){
         char c = s.charAt(i);
         switch(c){
            case '[':
            case '{':
               sb.append(c);
               if(i < s.length()-1 && s.charAt(i+1)==match(c)){ //treat [] and {} as special (lump them together, no newline)
                  i++;
                  sb.append(s.charAt(i));
               }
               else{
                  sb.append('\n');
                  tabs.append('\t');
                  sb.append(tabs);
               }
               break;
            case ']':
            case '}':
               sb.append('\n');
               if(tabs.length() > 0) tabs.deleteCharAt(tabs.length()-1);
               sb
                 .append(tabs)
                 .append(c);
               break;
            case ',':
               boolean wasNumber = true;
               for(int j = i-1; j >= 0; j--){
                  char bc = s.charAt(j);
                  if(bc == ',' || bc == '[' || bc == '{') break;
                  if(!(partOfNumber(bc) || Character.isWhitespace(bc))){
                     wasNumber = false;
                     break;
                  }
               }
               if(!wasNumber){
                  sb.append(c).append('\n').append(tabs);
               }
               else
                  sb.append(c);
               break;
            default:
               sb.append(c);
         }
      }
      return sb.toString();
   }
   
   private static char match(char c){
      switch(c){
         case '[': return ']';
         case '{': return '}';
         case ']': return '[';
         case '}': return '{';
      }
      return '\0';
   }
   
   private static boolean partOfNumber(char c){
      switch(c){
         case '0':
         case '1':
         case '2':
         case '3':
         case '4':
         case '5':
         case '6':
         case '7':
         case '8':
         case '9':
         case '-':
         case '+':
         case '.':
            return true;
      }
      return false;
   }



   /*
    * copy-and-pasted in order to avoid upgrade dependencies 
    */
   
   /*
    * Licensed to the Apache Software Foundation (ASF) under one or more
    * contributor license agreements.  See the NOTICE file distributed with
    * this work for additional information regarding copyright ownership.
    * The ASF licenses this file to You under the Apache License, Version 2.0
    * (the "License"); you may not use this file except in compliance with
    * the License.  You may obtain a copy of the License at
    *
    *      http://www.apache.org/licenses/LICENSE-2.0
    *
    * Unless required by applicable law or agreed to in writing, software
    * distributed under the License is distributed on an "AS IS" BASIS,
    * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    * See the License for the specific language governing permissions and
    * limitations under the License.
    */

   /**
    * <p>Works with {@link ToStringBuilder} to create a "deep" <code>toString</code>.</p>
    *
    * <p>To use this class write code as follows:</p>
    *
    * <pre>
    * public class Job {
    *   String title;
    *   ...
    * }
    * 
    * public class Person {
    *   String name;
    *   int age;
    *   boolean smoker;
    *   Job job;
    *
    *   ...
    *
    *   public String toString() {
    *     return new ReflectionToStringBuilder(this, new RecursiveToStringStyle()).toString();
    *   }
    * }
    * </pre>
    *
    * <p>This will produce a toString of the format:
    * <code>Person@7f54[name=Stephen,age=29,smoker=false,job=Job@43cd2[title=Manager]]</code></p>
    * 
    * @since 3.2
    * @version $Id: RecursiveToStringStyle.java 1572875 2014-02-28 08:34:55Z britter $
    */
   public static class RecursiveToStringStyle extends ToStringStyle {

       /**
        * Required for serialization support.
        * 
        * @see java.io.Serializable
        */
       private static final long serialVersionUID = 1L;

       /**
        * <p>Constructor.</p>
        */
       public RecursiveToStringStyle() {
           super();
       }

       @Override
       public void appendDetail(StringBuffer buffer, String fieldName, Object value) {
           if (!ClassUtils.isPrimitiveWrapper(value.getClass()) &&
               !String.class.equals(value.getClass()) &&
               accept(value.getClass())) {
               buffer.append(ReflectionToStringBuilder.toString(value, this));
           } else {
               super.appendDetail(buffer, fieldName, value);
           }
       }

       @Override
       protected void appendDetail(StringBuffer buffer, String fieldName, Collection<?> coll) {
           appendClassName(buffer, coll);
           appendIdentityHashCode(buffer, coll);
           appendDetail(buffer, fieldName, coll.toArray());
       }
       
       /**
        * Returns whether or not to recursively format the given <code>Class</code>.
        * By default, this method always returns {@code true}, but may be overwritten by
        * sub-classes to filter specific classes.
        *
        * @param clazz
        *            The class to test.
        * @return Whether or not to recursively format the given <code>Class</code>.
        */
       protected boolean accept(final Class<?> clazz) {
           return true;
       }
   }


}
