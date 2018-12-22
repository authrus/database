package com.authrus.database.engine.io;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class FilePathBuilder {   
   
   private static final String DATE_PATTERN = "yyyyMMddHHmmssSSS"; 

   private final DateFormat format;
   private final File template; 
   private final File parent;
   private final String name;
   
   public FilePathBuilder(String directory, String name) {
      this(directory, name, DATE_PATTERN);
   }
   
   public FilePathBuilder(String directory, String name, String pattern) {
      this.format = new SimpleDateFormat(pattern);
      this.parent = new File(directory, name);
      this.template = new File(parent, name);
      this.name = name;
   }
   
   public synchronized FilePath createFile() throws IOException {
      long time = System.currentTimeMillis();
      String prefix = template.getCanonicalPath();      
      String suffix = format.format(time);;
      String location = String.format("%s.%s", prefix, suffix);
      Path path = Paths.get(location);
      File file = path.toFile();
      File directory = file.getParentFile();
      
      if(!directory.exists()) {
         directory.mkdirs();
      }
      return new FilePath(file, name, time); 
   }
}
