package com.authrus.database.engine.io;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class FileHandle {

   private final ReferenceUpdater updater;
   
   public FileHandle(FilePointer pointer) {
      this.updater = new ReferenceUpdater(pointer);
   }
   
   public synchronized FileAppender open() throws IOException {
      FileAppender appender = updater.get();
      
      if(!appender.current()) {
         return updater.update();
      }
      return appender;
   }
   
   public synchronized long length() throws IOException {
      return updater.length();
   }
   
   public synchronized void close() throws IOException {
      updater.close();
   }
   
   private class ReferenceUpdater {      

      private final AtomicReference<FileAppender> current;
      private final FilePointer pointer;
      
      public ReferenceUpdater(FilePointer pointer) {
         this.current = new AtomicReference<FileAppender>();
         this.pointer = pointer;
      }
      
      public FileAppender get() throws IOException {
         FileAppender appender = current.get();
         
         if(appender == null) {
            return update();
         }
         return appender;
      }        
      
      public FileAppender update() throws IOException {
         FileAppender appender = create();
         
         if(appender != null) {
            current.set(appender);
         }
         return appender;
      }
      
      private FileAppender create() throws IOException {
         FileAppender appender = current.get();
         FilePath path = pointer.current();  
         
         if(appender != null) {
            appender.close();
         }
         return new FileAppender(pointer, path);
      }
      
      public long length() throws IOException {
         FileAppender appender = get();
         
         if(appender != null) {
            return appender.length();
         }
         return 0;
      }
      
      public void close() throws IOException {
         FileAppender appender = current.get();
         
         if(appender != null) {
            appender.close();
         }
      }
   }
      
}
