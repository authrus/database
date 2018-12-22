package com.authrus.database.engine.io;

import java.io.IOException;
import java.io.InputStream;

import com.authrus.database.engine.io.DataBlock;
import com.authrus.database.engine.io.DataBlockConsumer;

public class DataBlockInputStream extends InputStream {
      
   private DataBlockConsumer consumer;
   private boolean closed;
   private byte[] buffer;
   private byte[] swap;   
   private int count;
   private int read;
   private long wait;
   
   public DataBlockInputStream(DataBlockConsumer consumer) {
      this(consumer, 20000);
   }
   
   public DataBlockInputStream(DataBlockConsumer consumer, long wait) {     
      this.buffer = new byte[0];
      this.swap = new byte[1];
      this.consumer = consumer;         
      this.wait = wait;
   }

   @Override
   public int read() throws IOException {
      if(closed) {
         throw new IOException("Stream has been closed");
      }
      int count = read(swap);
      
      if(count == -1) {
         return -1;
      }
      return swap[0]&0xff;
   }

   @Override
   public int read(byte[] array, int off, int length) throws IOException {
      if(closed) {
         throw new IOException("Stream has been closed");
      }
      int remaining = count - read;
      
      if(remaining == 0) {
         int size = fill();
         
         if(size == -1) {
            return -1;
         }
         remaining = size;
      }
      int size = Math.min(remaining, length);

      if(size > 0) {
         System.arraycopy(buffer, read, array, off, size);
         read += size;
      }
      return size;
   }
   
   protected int fill() throws IOException {
      if(closed) {
         throw new IOException("Stream has been closed");
      }
      while(count - read <= 0) {
         DataBlock next = consumer.read(wait); // does this really work!!!!???? 
         
         if(next == null) {
            return -1;
         }
         byte[] data = next.getData();
         
         if(data.length > 0) {
            count = next.getLength();
            read = next.getOffset();
            buffer = data;            
         }
      }
      return count - read;      
   }
   
   @Override
   public int available() throws IOException {
      if(closed) {
         throw new IOException("Stream has been closed");
      }
      return count - read;
   }
   
   @Override
   public void close() throws IOException {
      if(!closed){
         buffer = null;
         consumer = null;
         closed = true;
      }
   }   
}
