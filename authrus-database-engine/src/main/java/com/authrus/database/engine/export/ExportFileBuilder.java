package com.authrus.database.engine.export;

import java.io.File;
import java.io.IOException;

import com.authrus.database.common.time.TimeStampBuilder;

public class ExportFileBuilder {

   private final TimeStampBuilder builder;   
   
   public ExportFileBuilder() {     
      this.builder = new TimeStampBuilder();     
   }
   
   public File createFile(File root, String table) throws IOException {
      String time = builder.createTimeStamp();
      String name = String.format("%s.%s.csv.gz", table, time);
      String file = name.toLowerCase();
      
      if(!root.exists()) {
         root.mkdirs();
      }
      return new File(root, file);
   }
}
