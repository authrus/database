package com.authrus.database.engine.io;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class FilePointer {
   
   private final AtomicReference<FilePath> pointer;
   private final FilePathComparator comparator;   
   private final FilePathBuilder builder;
   private final FilePathScanner scanner;
   private final FileHandle handle;
   private final String directory;
   
   public FilePointer(String directory, String name) {
      this.builder = new FilePathBuilder(directory, name);
      this.scanner = new FilePathScanner(directory, name);
      this.pointer = new AtomicReference<FilePath>();
      this.comparator = new FilePathComparator();
      this.handle = new FileHandle(this);
      this.directory = directory;
   }
   
   public FileHandle handle() throws IOException {
      return handle;
   }   
   
   public FilePath start() throws IOException {
      FilePath path = builder.createFile();
      
      if(path == null) {
         throw new IOException("Could not create file in '" + directory  +"'");
      }
      pointer.set(path);      
      return path;
   }    
   
   public FilePath current() throws IOException {
      FilePath path = pointer.get();
      
      if(path == null) {
         return next();
      }
      return path;
   }  

   public FilePath next() throws IOException {
      List<FilePath> paths = scanner.listFiles();
      FilePath current = pointer.get();
      
      for(FilePath path : paths) {
         if(current == null) {
            pointer.set(path);
            return path;  
         }
         int comparison = comparator.compare(path, current);
               
         if(comparison > 0) {                              
            pointer.set(path);
            return path;              
         } 
      }
      FilePath next = builder.createFile();
      
      if(next == null) {
         throw new IOException("Could not create file in '" + directory  +"'");
      }
      pointer.set(next);           
      return next;
   }
   
   public FilePath previous() throws IOException {
      List<FilePath> paths = scanner.listFiles();
      FilePath current = pointer.get();
      
      if(current != null) {
         FilePath previous = current;
         
         for(FilePath path : paths) {          
            int comparison = comparator.compare(path, current);
            
            if(comparison < 0) {                          
               pointer.set(path); 
               previous = path;
            }
            if(comparison >= 0) {
               break;
            }
         }
         if(previous != current) {
            return previous;            
         }        
      }
      return null;
   }
}
 