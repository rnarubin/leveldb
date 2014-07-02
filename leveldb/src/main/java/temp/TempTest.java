package temp;

import static org.iq80.leveldb.impl.Iq80DBFactory.bytes;
import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.util.VariableLengthQuantity;

public class TempTest
{
   
   private static final String num[] = {"zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine"};

   public static void main(String[] args)
   {
      Options options = new Options();
      
      options.createIfMissing(true);
      try(DB db = factory.open(new File("example"), options)){
              
         for(int i = 0; i < 10; i++){
            db.put(bytes(num[i]), bytes(""+i));
         }
              

         DBIterator iter = db.iterator();
         while(iter.hasNext()){
            Entry<byte[], byte[]> e = iter.next();
            System.out.println(new String(e.getKey())+", "+new String(e.getValue()));
         }
         
         
      }catch(IOException e){
         e.printStackTrace();
      }
   }

}
