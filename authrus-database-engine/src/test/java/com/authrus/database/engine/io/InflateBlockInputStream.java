package com.authrus.database.engine.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

public class InflateBlockInputStream extends InputStream {
      
   private DataBlockInputStream stream;
   private GZIPInputStream inflater;
   private boolean closed;
   private int buffer;
   private byte[] swap; 
   
   public InflateBlockInputStream(DataBlockInputStream stream) throws IOException {
      this(stream, 2048);
   }
   
   public InflateBlockInputStream(DataBlockInputStream stream, int buffer) throws IOException {
      this.swap = new byte[1];
      this.stream = stream;
      this.buffer = buffer;
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
      int count = inflate(array, off, length);
      
      while(count <= 0) {
         int remaining = stream.fill();
         
         if(remaining > 0) {
            inflater = new GZIPInputStream(stream, buffer); // restart stream
         } else {
            return -1;
         }         
         count = inflate(array, off, length);
      }
      return count;
   }
   
   protected int inflate(byte[] array, int off, int length) throws IOException {
      if(closed) {
         throw new IOException("Stream has been closed");
      }
      try {
         if(inflater != null) {
            return inflater.read(array, off, length);
         }
      } catch(Exception cause) {
         return 0; // ignore bad blocks!!
      }
      return 0;
   }
   
   @Override
   public int available() throws IOException {
      if(closed) {
         throw new IOException("Stream has been closed");
      }
      int count = stream.available(); // ensure we do not block
      
      if(count > 0) {
         return inflater.available();
      }
      return 0;
   }
   
   @Override
   public void close() throws IOException {
      if(!closed){
         stream.close();
         inflater = null;
         closed = true;
      }
   }   
}
