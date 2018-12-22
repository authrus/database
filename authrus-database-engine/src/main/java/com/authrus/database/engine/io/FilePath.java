package com.authrus.database.engine.io;

import java.io.File;

public class FilePath {

   private final String name;
   private final File file;
   private final long time;
   
   public FilePath(File file, String name, long time) {  
      this.name = name;
      this.time = time;
      this.file = file;
   }
   
   public long getTime() {
      return time;
   }
   
   public File getFile() {
      return file;
   }
   
   public String getName() {
      return name;
   }
   
   @Override
   public String toString() {
      return file.getAbsolutePath();
   }
}
