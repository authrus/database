package com.authrus.database.engine.io;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FilePathScanner {

   private final FilePathComparator comparator;
   private final FilePathConverter converter;
   private final File directory;
   private final String filter;

   public FilePathScanner(String directory) {
      this(directory, null);
   }
   
   public FilePathScanner(String directory, String filter) {
      this.converter = new FilePathConverter();
      this.comparator = new FilePathComparator();
      this.directory = new File(directory);
      this.filter = filter;
   }
   
   public List<FilePath> listFiles() throws IOException {      
      List<FilePath> result = new ArrayList<FilePath>();
      
      if(directory.exists()) {
         File[] files = directory.listFiles();
         
         for(File file : files) {
            if(file.isDirectory()) {               
               if(filter != null) {
                  String name = file.getName();
                  
                  if(!name.equals(filter)) {
                     continue;
                  }                  
               }
               List<FilePath> matches = listFiles(file);
                 
               if(!matches.isEmpty()) {
                  result.addAll(matches);
               }                             
            }
         }      
         if(!result.isEmpty()) {
            Collections.sort(result, comparator);
         }
      }
      return result;         
   }
   
   private List<FilePath> listFiles(File directory) throws IOException {
      List<FilePath> result = new ArrayList<FilePath>();
      
      if(directory.exists()) {
         File[] files = directory.listFiles();
         
         for(File file : files) {            
            if(file.isFile()) {
               String name = file.getName();
               FilePath path = converter.convert(directory, name);
               
               if(path != null) {
                  result.add(path);
               }
            } else if(file.isDirectory()) {
               List<FilePath> list = listFiles(file);
               
               if(list != null) {
                  result.addAll(list);
               }
            }            
         }        
      }
      return result;         
   }
}
