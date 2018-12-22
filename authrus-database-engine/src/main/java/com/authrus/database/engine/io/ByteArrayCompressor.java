package com.authrus.database.engine.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class ByteArrayCompressor {
   
   private final byte[] chunk;
   private final int header;
   
   public ByteArrayCompressor() {
      this(0);
   }   

   public ByteArrayCompressor(int header) {
      this.chunk = new byte[8192];
      this.header = header;
   }
   
   public byte[] read(byte[] array) {
      return read(array, 0, array.length);
   }

   public byte[] read(byte[] array, int off, int length) {
      try {
         ByteArrayOutputStream buffer = new ByteArrayOutputStream();
         ByteArrayInputStream source = new ByteArrayInputStream(array, off + header, length - header);         
         GZIPInputStream decoder = new GZIPInputStream(source, chunk.length);
         
         if(length > 0) {
            int count = 0;
            
            while((count = decoder.read(chunk)) != -1) {
               buffer.write(chunk, 0, count);
            }
            decoder.close();
         }
         return buffer.toByteArray();
      } catch(Exception e) {
         throw new IllegalStateException("Could not decompress data of length " + length, e);
      }
   }

   public byte[] write(byte[] array) {
      return write(array, 0, array.length);
   }
   
   public byte[] write(byte[] array, int off, int length) {
      try {
         ByteArrayOutputStream buffer = new ByteArrayOutputStream();         
         
         for(int i = 0; i < header; i++) {
            buffer.write(0);
         }
         if(length > 0) {
            GZIPOutputStream encoder = new GZIPOutputStream(buffer, chunk.length);         
    
            encoder.write(array, off, length);
            encoder.close();
         }
         return buffer.toByteArray();
      } catch(Exception e) {
         throw new IllegalStateException("Could not compress data of length " + length, e);
      }
   }
}
