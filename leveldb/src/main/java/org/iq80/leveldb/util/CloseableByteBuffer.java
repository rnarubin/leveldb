package org.iq80.leveldb.util;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class CloseableByteBuffer implements Closeable
{
   /*
    * actually extending ByteBuffer is not permitted, user polymorphism was not an intended feature of the class it seems
    */
   public abstract CloseableByteBuffer put(byte b);
   public abstract CloseableByteBuffer put(byte[] b);
   public abstract CloseableByteBuffer put(ByteBuffer b);
   public abstract CloseableByteBuffer putInt(int b);
   
   public abstract void close() throws IOException;
}
