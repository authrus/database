package com.authrus.database.engine.io.read;

import static com.authrus.database.engine.OperationType.BATCH;
import static com.authrus.database.engine.OperationType.BEGIN;
import static com.authrus.database.engine.OperationType.COMMIT;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.authrus.database.engine.OperationType;

public class BatchOperationProcessor {

   private final AtomicReference<OperationType> reference;
   private final List<ChangeOperation> batch;
   private final ChangeScheduler scheduler;

   public BatchOperationProcessor(ChangeScheduler scheduler) {
      this.reference = new AtomicReference<OperationType>(COMMIT);
      this.batch = new ArrayList<ChangeOperation>();
      this.scheduler = scheduler;
   }
   
   public void process(ChangeOperation operation, OperationType type) { // combine operations in to a single batch/transaction
      OperationType previous = reference.get();
      
      if(previous.equals(COMMIT)) {
         if(!type.equals(BATCH) && !type.equals(BEGIN)) {
            throw new IllegalStateException("Change is not in a batch or a transaction");
         }
      }
      if(type.equals(COMMIT)) {
         List<ChangeOperation> list = new ArrayList<ChangeOperation>(batch);
         ChangeOperation task = new BatchOperation(list);

         list.add(operation);
         scheduler.schedule(task);
         batch.clear();
      } else {
         batch.add(operation);
      }
      reference.set(type);
   }
}
