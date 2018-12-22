package com.authrus.database.engine.io.write;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileLog implements ChangeLog {
   
   private static final Logger LOG = LoggerFactory.getLogger(FileLog.class);

   private volatile Map<String, FileLogAppender> appenders;
   private volatile FileLogAllocator allocator;
   private volatile AtomicBoolean enable;

   public FileLog(String directory, String owner, int capacity) {
      this(directory, owner, capacity, 1000);
   }
   
   public FileLog(String directory, String owner, int capacity, int group) {
      this.allocator = new FileLogAllocator(directory, owner, capacity, group);
      this.appenders = new ConcurrentHashMap<String, FileLogAppender>();
      this.enable = new AtomicBoolean();
   }

   @Override
   public void log(ChangeRecord record) {
      try {
         if(enable.get()) {
            String name = record.getTable();                          
            FileLogAppender appender = appenders.get(name);
            
            if(appender == null) {
               appender = allocator.allocate(name);
            }            
            appender.append(record);
         }
      } catch(Exception e) {
         LOG.warn("Could not log record", e);
      }
   }
   
   @Override
   public void start() {
      enable.set(true);
   }
   
   @Override
   public void stop() {
      enable.set(false);
   }  
   
   private class FileLogAllocator {

      private final String directory;
      private final String owner;
      private final int capacity;
      private final int group;
      
      public FileLogAllocator(String directory, String owner, int capacity, int group) {
         this.directory = directory;
         this.capacity = capacity;
         this.group = group;
         this.owner = owner;
      }

      public synchronized FileLogAppender allocate(String name) {
         FileLogAppender current = appenders.get(name);

         if (current == null) {
            FileLogAppender appender = new FileLogAppender(directory, owner, name, capacity, group);
            Map<String, FileLogAppender> copy = new HashMap<String, FileLogAppender>();
            
            copy.putAll(appenders);
            copy.put(name, appender);
            appenders = copy;
            return appender;
         }
         return current;
      }
   }
}

