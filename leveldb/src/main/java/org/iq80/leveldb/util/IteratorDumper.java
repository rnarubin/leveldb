
package org.iq80.leveldb.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.RecursiveToStringStyle;
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
}
