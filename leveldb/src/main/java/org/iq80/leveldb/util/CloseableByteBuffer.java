package org.iq80.leveldb.util;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface CloseableByteBuffer extends Closeable
{
   /*
    * actually extending ByteBuffer is not permitted, user polymorphism was not an intended feature of the class it seems
    */
   public CloseableByteBuffer put(byte b);
   public CloseableByteBuffer put(byte[] b);
   public CloseableByteBuffer put(ByteBuffer b);
   public CloseableByteBuffer putInt(int b);
   
   public void close() throws IOException;
}
