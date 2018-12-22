package com.authrus.database.engine.io;

import java.io.File;

public class FileRegion implements DataBlock {
   
   private final FileRegionReader reader;
   private final FilePath path;
   private final long seek;
   private final int size;
   
   public FileRegion(FilePath path, long seek, int size) {    
      this.reader = new FileRegionReader(path);
      this.path = path;
      this.seek = seek;
      this.size = size;
   }
   
   public File getFile() {
      return path.getFile();
   }    

   @Override
   public byte[] getData() { 
      try {
         return reader.readRegion(seek, size);
      } catch(Exception e) {
         throw new IllegalStateException("Could not read region from " + path);
      }      
   }  

   @Override
   public String getName() {
      return path.getName();
   }   
   
   @Override
   public long getTime() {
      return path.getTime();
   }    

   @Override
   public int getOffset() {
      return 0;
   }

   @Override
   public int getLength() {
      return size;
   }
   
   @Override
   public String toString() {
      return String.format("%s -> %s", path, size);
   }
}
