package com.authrus.database.engine.io.replicate;

import java.io.File;

import com.authrus.database.engine.io.FilePath;
import com.authrus.database.engine.io.FilePathConverter;

public class DirectoryCleaner {
   
   private final FilePathConverter converter;
   private final File directory;
   private final long expiry;

   public DirectoryCleaner(String directory) {
      this(directory, 600000);
   }
   
   public DirectoryCleaner(String directory, long expiry) {
      this.converter = new FilePathConverter();
      this.directory = new File(directory);
      this.expiry = expiry;
   }
   
   public void clean() throws Exception {
      File[] files = directory.listFiles();
      
      if(files != null) {
         for(File file : files) {
            if(file.isFile()) {
               String name = file.getName();
               File parent = file.getParentFile();
               FilePath path = converter.convert(parent, name);
               
               if(path != null) {
                  long time = System.currentTimeMillis();
                  long length = file.length();
                  long modified = file.lastModified();
                  long age = time - modified;
                  
                  if(length == 0 && age > expiry) {  // clean up empties                
                     file.delete();
                  }
               }               
            }
         }
      }
   }
}
