package com.authrus.database.terminal.session;

import java.lang.management.ManagementFactory;

import com.sun.management.ThreadMXBean;
import com.authrus.database.common.MemoryUnit;

public class MemoryAnalyzer {    
   
   private final ThreadMemoryLocal local; 

   public MemoryAnalyzer() {
      this.local = new ThreadMemoryLocal();
   }

   public void start() {
      ThreadMemory memory = local.get();
      
      if(memory == null) {
         throw new IllegalStateException("Could not start analyzer");
      }
      memory.before();
   }
   
   public void stop() {
      ThreadMemory memory = local.get();
      
      if(memory == null) {
         throw new IllegalStateException("Could not stop analyzer");
      }
      memory.after();   
   }
   
   public String change() {
      ThreadMemory memory = local.get();
      
      if(memory == null) {
         throw new IllegalStateException("Could not determine change");
      }      
      long value = memory.change();
      
      if(value > 0) {
         return MemoryUnit.format(value);
      }
      return MemoryUnit.format(0, MemoryUnit.BYTE);
   }   
   
   public String allocated() {
      ThreadMemory memory = local.get();
      
      if(memory == null) {
         throw new IllegalStateException("Could not determine allocation");
      }
      long value = memory.allocated();
      
      if(value > 0) {
         return MemoryUnit.format(value);
      }
      return MemoryUnit.format(0, MemoryUnit.BYTE);
   }    
   
   private static class ThreadMemoryLocal extends ThreadLocal<ThreadMemory> {      
      
      private final ThreadMXBean analyzer;

      public ThreadMemoryLocal() {
         this.analyzer = (ThreadMXBean) ManagementFactory.getThreadMXBean();      
      }

      @Override
      protected ThreadMemory initialValue() {
         return new ThreadMemory(analyzer);
      }
   }
   
   private static class ThreadMemory {
      
      private volatile ThreadMXBean analyzer;
      private volatile Thread thread;
      private volatile long before;
      private volatile long after;

      public ThreadMemory(ThreadMXBean analyzer) {    
         this.thread = Thread.currentThread();
         this.analyzer = analyzer;
      }
      
      public void before() {
         Thread current = Thread.currentThread();
         long key = current.getId();
         
         if(current != thread) {
            throw new IllegalStateException("This is bound to thread " + thread + " not " + current);
         }
         before = analyzer.getThreadAllocatedBytes(key);
      }
      
      public void after() {
         Thread current = Thread.currentThread();
         long key = current.getId();
         
         if(current != thread) {
            throw new IllegalStateException("This is bound to thread " + thread + " not " + current);
         }
         after = analyzer.getThreadAllocatedBytes(key);
      }
      
      public long allocated() {
         Thread current = Thread.currentThread();
         long key = current.getId();
         
         if(current != thread) {
            throw new IllegalStateException("This is bound to thread " + thread + " not " + current);
         }
         return analyzer.getThreadAllocatedBytes(key);                 
      }      
   
      public long change() {
         return after - before;
      }      
      
      @Override
      public String toString() {
         return thread.toString();
      }
   }
}
