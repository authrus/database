package com.authrus.database.engine.io.replicate;

import java.io.File;

import com.authrus.database.engine.io.FilePath;
import com.authrus.database.engine.io.FileSeeker;

public class PositionSeeker implements FileSeeker {
   
   private final Position position;
   
   public PositionSeeker(Position position) {
      this.position = position;
   }

   @Override
   public boolean accept(FilePath path) {
      File file = path.getFile();
      String name = path.getName();
      long time = path.getTime();
      long start = position.getTime(name);
      
      if(time >= start) {
         return file.exists();
      }      
      return false;
   }

}
