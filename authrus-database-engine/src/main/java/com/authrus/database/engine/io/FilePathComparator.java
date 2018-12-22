package com.authrus.database.engine.io;

import java.util.Comparator;

public class FilePathComparator implements Comparator<FilePath> {
   
   private final boolean oldestFirst;

   public FilePathComparator() {
      this(true);
   }
   
   public FilePathComparator(boolean oldestFirst) {
      this.oldestFirst = oldestFirst;
   }

   @Override
   public int compare(FilePath left, FilePath right) {
      Long leftTime = left.getTime();
      Long rightTime = right.getTime();
      
      if(oldestFirst) {
         return leftTime.compareTo(rightTime);    
      }
      return rightTime.compareTo(leftTime);             
   }   

}
