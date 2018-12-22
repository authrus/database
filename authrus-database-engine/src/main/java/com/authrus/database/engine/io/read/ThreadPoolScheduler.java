package com.authrus.database.engine.io.read;

import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadPoolScheduler implements ChangeScheduler {

   private static final Logger LOG = LoggerFactory.getLogger(ChangeScheduler.class);
   
   private final ChangeAssembler assembler;
   private final Executor executor;

   public ThreadPoolScheduler(ChangeAssembler assembler) {
      this(assembler, null);
   }
   
   public ThreadPoolScheduler(ChangeAssembler assembler, Executor executor) {
      this.assembler = assembler;
      this.executor = executor;
   }

   @Override
   public void schedule(ChangeOperation operation) {
      ChangeCall call = new ChangeCall(operation);
      
      if(executor != null) {
         executor.execute(call); 
      } else {
         call.run();
      }
   }
   
   private class ChangeCall implements Runnable {
      
      private final ChangeOperation operation;
      
      public ChangeCall(ChangeOperation operation) {
         this.operation = operation;
      }
      
      @Override
      public void run() {
         try {
            operation.execute(assembler);
         } catch(Exception e){
            LOG.warn("Error executing operation", e);
         }
      }
   }
}
