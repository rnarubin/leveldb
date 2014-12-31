package org.iq80.leveldb.util;

import java.nio.ByteBuffer;

public interface CloseableByteBuffer extends AutoCloseable
{
   /*
    * actually extending ByteBuffer is not permitted, user polymorphism was not an intended feature of the class it seems
    */
   public CloseableByteBuffer put(byte b) throws Exception;
   public CloseableByteBuffer put(byte[] b) throws Exception;
   public CloseableByteBuffer put(ByteBuffer b) throws Exception;
   public CloseableByteBuffer putInt(int b) throws Exception;
}
