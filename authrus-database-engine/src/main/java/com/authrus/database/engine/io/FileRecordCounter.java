package com.authrus.database.engine.io;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class FileRecordCounter implements DataRecordCounter {

   private final FilePointer pointer;
   private final AtomicLong counter;
   private final String owner;
   
   public FileRecordCounter(FilePointer pointer, String owner) {
      this.counter = new AtomicLong();
      this.pointer = pointer; 
      this.owner = owner;
   }
   
   @Override
   public String getOrigin() throws IOException {
      return owner;
   }
   
   @Override
   public String getTable() throws IOException {
      FilePath path = pointer.current();
      
      if(path == null) {
         throw new IOException("Could not determine current record");
      }
      return path.getName();
   }   

   @Override
   public long getTime() throws IOException {
      FilePath path = pointer.current();
      
      if(path == null) {
         throw new IOException("Could not determine current record");
      }
      return path.getTime();
   }

   @Override
   public long getNext() throws IOException {
      return counter.getAndIncrement();
   }
   
   @Override
   public long getCurrent() throws IOException {
      return counter.get();
   }
}
