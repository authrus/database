package com.authrus.database.engine.io;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

public class DeflateBlockOutputStream extends OutputStream {
   
   private GZIPOutputStream deflater;
   private OutputStream stream;
   private boolean closed;
   private byte[] swap;
   private byte[] buffer;
   private int count;
   
   public DeflateBlockOutputStream(OutputStream stream) {
      this(stream, 8192);
   }
   
   public DeflateBlockOutputStream(OutputStream stream, int block) {
      this.buffer = new byte[block];
      this.swap = new byte[1];
      this.stream = stream;       
   }
   
   @Override
   public void write(int octet) throws IOException {
      if(closed) {
         throw new IOException("Stream has been closed");
      }
      int capacity = buffer.length;
      int space = capacity - count;
      
      if(space > 0) {
         buffer[count++] = (byte)octet;
      } else {      
         swap[0] = (byte)octet;
         write(swap, 0, 1);
      }
   }
   
   @Override
   public void write(byte[] array, int off, int length) throws IOException {
      if(closed) {
         throw new IOException("Stream has been closed");
      }
      int capacity = buffer.length;
      
      if(deflater == null) {
         deflater = new GZIPOutputStream(stream, true); // expensive??
      }
      if(length > capacity) {
         deflater.write(buffer, 0, count);
         deflater.write(array, off, length);
         count = 0;
      } else {
         int space = capacity - count;
      
         if(space < length) {
            deflater.write(buffer, 0, count);
            System.arraycopy(array, off, buffer, 0, length);  
            count = length;
         } else {                     
            System.arraycopy(array, off, buffer, count, length);
            count += length;
         }
      }      
   }
   
   @Override
   public void flush() throws IOException {
      if(closed) {
         throw new IOException("Stream has been closed");
      }
      if(deflater != null) {
         if(count > 0) {
            deflater.write(buffer, 0, count);
            deflater.flush();
         } else {
            deflater.flush();
            deflater.finish(); // enable multiple blocks in one stream
            deflater = null;
         }
         count = 0;
      }
   }
   
   @Override
   public void close() throws IOException {
      if(!closed) {
         if(deflater != null) {           
            deflater.write(buffer, 0, count);            
            deflater.close();
         } else {
            stream.close();
         }
         deflater = null;
         closed =true;
      }
   }
}
