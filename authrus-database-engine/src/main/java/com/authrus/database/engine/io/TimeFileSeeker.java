package com.authrus.database.engine.io;

import java.io.File;

public class TimeFileSeeker implements FileSeeker {
   
   private final long start;
   
   public TimeFileSeeker() {
      this(0L);
   }
   
   public TimeFileSeeker(long start) {
      this.start = start;
   }

   @Override
   public boolean accept(FilePath path) {
      File file = path.getFile();
      long time = path.getTime();
      
      if(time >= start) {
         return file.exists();
      }
      return false;      
   }
}
