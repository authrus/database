package com.authrus.database.engine.io.write;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import com.authrus.database.common.thread.ThreadPoolFactory;
import com.authrus.database.engine.io.FileHandle;
import com.authrus.database.engine.io.FilePath;
import com.authrus.database.engine.io.FilePointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileLogAppender {   
   
   private static final Logger LOG = LoggerFactory.getLogger(FileLogAppender.class);

   private final BatchRecordProcessor processor;
   private final FileLogListener listener;
   private final BatchAppender appender;
   private final ThreadFactory factory;
   private final FilePointer pointer;
   private final AtomicBoolean alive;

   private final int capacity; 

   public FileLogAppender(String directory, String owner, String name, int capacity) {
      this(directory, owner, name, capacity, 81920);
   }
   
   public FileLogAppender(String directory, String owner, String name, int capacity, int group) {
      this.factory = new ThreadPoolFactory(BatchAppender.class);  
      this.pointer = new FilePointer(directory, name);
      this.listener = new FileLogListener(pointer, owner, group);
      this.processor = new BatchRecordProcessor(listener, pointer, owner, name);
      this.appender = new BatchAppender(name);
      this.alive = new AtomicBoolean();
      this.capacity = capacity;
   }
   
   public void append(ChangeRecord record) throws IOException {
      if(alive.compareAndSet(false, true)) {
         Thread thread = factory.newThread(appender);
         
         pointer.start(); // always start with fresh file
         thread.start();
      }
      appender.append(record);
   } 
   
   private class BatchAppender implements Runnable {
      
      private final BlockingQueue<ChangeRecord> records;
      private final List<ChangeRecord> batch;
      private final String name;
      private final long frequency;

      public BatchAppender(String name) {
         this(name, 1000);
      }
      
      public BatchAppender(String name, long frequency) {
         this.records = new LinkedBlockingQueue<ChangeRecord>();
         this.batch = new ArrayList<ChangeRecord>();
         this.frequency = frequency;
         this.name = name;
      }      
      
      public void append(ChangeRecord record) {
         records.offer(record);
      }

      public void run() {         
         try {   
            while(alive.get()) {
               ChangeRecord record = records.poll(frequency, MILLISECONDS);
                
               if(record != null) {
                  batch.add(record);
                  records.drainTo(batch);
                  
                  process();
               } else {
                  close();
               }
               batch.clear();
            }
         } catch(Exception e) {
            LOG.warn("Error appending records for '" + name + "'", e);
         } finally {
            alive.set(false);
         }
      }      
      
      private void process() throws IOException {
         if(processor.process(batch)) { // roll only when processed
            FileHandle handle = pointer.handle(); 
            
            if(handle != null) {
               long length = handle.length();
               
               if(length > capacity) {
                  FilePath next = pointer.next();
                  File file = next.getFile();
                  
                  if(file.exists()) {
                     throw new IOException("File '" + file + "' already exists");
                  }         
               }
            }        
         }
      }
      
      private void close() throws IOException {
         FileHandle handle = pointer.handle();

         if(handle != null) {
            handle.close();
         }
      } 
   }
}
