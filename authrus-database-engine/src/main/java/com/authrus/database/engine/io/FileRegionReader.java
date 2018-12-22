package com.authrus.database.engine.io;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FileRegionReader {
   
   private final FilePath path;
   
   public FileRegionReader(FilePath path) {
      this.path = path;
   }
   
   public byte[] readRegion(long seek) throws IOException {
      File file = path.getFile();     
      
      if(!file.exists()) {
         throw new FileNotFoundException("File " + file + " does not exist");
      }
      RandomAccessFile source = new RandomAccessFile(file, "r");
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      byte[] chunk = new byte[1025];
      
      try {
         if(seek > 0) {
            source.seek(seek);
         }           
         int count = 0;
         
         while((count = source.read(chunk)) != -1) {
            buffer.write(chunk, 0, count);
         }
         return buffer.toByteArray();
      } finally {
         source.close();
      }
   }

   public byte[] readRegion(long seek, int size) throws IOException {
      File file = path.getFile();
      
      if(!file.exists()) {
         throw new FileNotFoundException("File " + file + " does not exist");
      }
      RandomAccessFile source = new RandomAccessFile(file, "r");
      byte[] chunk = new byte[size];
      
      try {
         if(seek > 0) {
            source.seek(seek);
         }          
         int count = source.read(chunk);
         
         if(count != size) {
            throw new IOException("Could only read " + count +" of " + size + " from " + file);          
         }
         return chunk;       
      } finally {
         source.close();
      } 
   }
}
