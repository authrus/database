package com.authrus.database.engine.io.write;

import static com.authrus.database.engine.OperationType.BATCH;
import static com.authrus.database.engine.OperationType.BEGIN;
import static com.authrus.database.engine.OperationType.COMMIT;
import static com.authrus.database.engine.OperationType.DROP;
import static com.authrus.database.engine.OperationType.ROLLBACK;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.authrus.database.engine.OperationType;
import com.authrus.database.engine.io.FilePointer;

public class BatchRecordProcessor {   

   private final AtomicReference<ChangeRecordBatch> reference;
   private final ChangeRecordListener listener;
   private final ChangeRecordWriter writer;
   private final BatchRecordBuilder builder;  
   private final FilePointer pointer;
   private final ChangeRecord commit;
   private final String owner; 
   
   public BatchRecordProcessor(ChangeRecordListener listener, FilePointer pointer, String owner, String table) {
      this.writer = new CommitRecordWriter(owner);
      this.commit = new ChangeRecord(writer, COMMIT, owner, table);      
      this.builder = new BatchRecordBuilder(owner, table);
      this.reference = new AtomicReference<ChangeRecordBatch>();
      this.listener = listener;    
      this.pointer = pointer;
      this.owner = owner;
   }
   
   public boolean process(List<ChangeRecord> records) throws IOException {
      for(ChangeRecord record : records){
         String origin = record.getOrigin();
         ChangeRecordBatch batch = reference.get();
         
         if(batch == null) {
            begin(record);
         } else { 
            String current = batch.getOrigin();
         
            if(!current.equals(origin)) { // user changed        
               commit();
               begin(record);
            } else {
               update(record);
            }
         }           
      }
      return ready();
   }
   
   private void begin(ChangeRecord record) throws IOException {
      String origin = record.getOrigin();
      OperationType type = record.getType();
      ChangeRecordBatch batch = builder.create(type, origin); // new batch
      List<ChangeRecord> records = batch.getRecords();
      
      if(type.equals(COMMIT)) {
         throw new IOException("Batch from '" + owner + "' is starting with a commit");
      }
      if(type.equals(ROLLBACK)) {
         throw new IOException("Batch from '" + owner + "' is starting with a commit");
      }      
      reference.set(batch);
      records.add(record);
   }
   
   private void update(ChangeRecord record) throws IOException {
      OperationType type = record.getType();
      ChangeRecordBatch batch = reference.get();      
      List<ChangeRecord> records = batch.getRecords();                
      
      if(type.equals(BEGIN)) {
         ChangeRecord first = records.get(0);         
         OperationType open = first.getType();  
         String origin = first.getOrigin();
         
         if(open.equals(BEGIN)) {
            throw new IOException("Transaction from '" + origin + "' was not committed");
         }
         if(!open.equals(BATCH)) {
            throw new IOException("Batch from '" + origin + "' was not started");
         }
         commit();
         begin(record);
      } else {
         records.add(record);
      
         if(type.equals(COMMIT)) {
            commit();
         } else if(type.equals(ROLLBACK)) {
            rollback();
         }
      }
   }
   
   private void rollback() throws IOException {
      ChangeRecordBatch batch = reference.get();
      
      if(batch == null) {
         throw new IOException("No transaction ready to rollback");
      }
      List<ChangeRecord> records = batch.getRecords();
      String origin = batch.getOrigin();
      int count = records.size();         

      if(count > 1) {
         ChangeRecord last = records.get(count -1);
         OperationType close = last.getType(); 
         
         if(!close.equals(ROLLBACK)) { 
            throw new IOException("Transaction from '" + origin + "' was not rolled back");         
         }
         reference.set(null); // delete everything  
      }
   }
   
   private void commit() throws IOException {
      ChangeRecordBatch batch = reference.get();
      
      if(batch == null) {
         throw new IOException("No transaction ready to commit");
      }
      List<ChangeRecord> records = batch.getRecords();
      String origin = batch.getOrigin();
      int count = records.size();         

      if(count > 1) {
         ChangeRecord first = records.get(0);
         ChangeRecord second = records.get(1); // first action
         ChangeRecord last = records.get(count -1);
         OperationType open = first.getType();
         OperationType action = second.getType();
         OperationType close = last.getType();
         
         if(open.equals(BEGIN)) {
            if(!close.equals(COMMIT)) { 
               throw new IOException("Transaction from '" + origin + "' was not committed");
            }
         } else {
            if(open.equals(BATCH)) {            
               if(!close.equals(COMMIT)) {               
                  if(!origin.equals(owner)) { // only current user does not have to commit
                     throw new IOException("Batch from '" + origin + "' was not committed");
                  }
                  records.add(commit); // commit if not committed
               }
            } else {                  
               throw new IOException("Batch from '" + origin + "' was not started");
            }               
         }
         if(action.equals(DROP)) { 
            pointer.next(); // force new file on drop!
         }
         listener.update(batch);         
      }
      reference.set(null);            
   }  
   
   private boolean ready() throws IOException {
      ChangeRecordBatch batch = reference.get();   
      
      if(batch != null) {
         List<ChangeRecord> records = batch.getRecords();                
         int count = records.size();         

         if(count > 0) {
            ChangeRecord first = records.get(0);
            ChangeRecord last = records.get(count -1);
            OperationType open = first.getType();
            OperationType close = last.getType(); 
            
            if(open.equals(BEGIN)) { // transaction
               if(!close.equals(COMMIT)) { 
                  return false;
               }
            }
            commit(); // commit any batch or transaction
         }
      }
      return true;
   }
}
 