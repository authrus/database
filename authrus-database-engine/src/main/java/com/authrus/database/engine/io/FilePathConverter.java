package com.authrus.database.engine.io;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class FilePathConverter {
   
   private static final String DATE_PATTERN = "yyyyMMddHHmmssSSS"; 
   
   private final DateFormat format; 
   private final String pattern;

   public FilePathConverter() {
      this(DATE_PATTERN);
   }
   
   public FilePathConverter(String pattern) {
      this.format = new SimpleDateFormat(pattern);       
      this.pattern = pattern;
   }

   public FilePath convert(File parent, String name) throws IOException {
      int require = pattern.length();      
      int index = name.indexOf('.');
      int length = name.length();
      
      if(index != -1 && length - index == require + 1) {
         File file = new File(parent, name);
         
         for(int i = index + 1; i < length; i++) {
            char next = name.charAt(i);
            
            if(next < '0' || next > '9') {
               return null;
            }
         }
         String prefix = name.substring(0, index);            
         String suffix = name.substring(index + 1);
         
         try {
            Date date = format.parse(suffix);
            long time = date.getTime();
         
            return new FilePath(file, prefix, time);
         } catch(Exception e) {
            return null;
         }         
      }
      return null;
   }
}
