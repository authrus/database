package com.authrus.database.engine.io.read;

import static com.authrus.database.engine.OperationType.BATCH;
import static com.authrus.database.engine.OperationType.BEGIN;
import static com.authrus.database.engine.OperationType.COMMIT;
import static com.authrus.database.engine.OperationType.CREATE;
import static com.authrus.database.engine.OperationType.DELETE;
import static com.authrus.database.engine.OperationType.DROP;
import static com.authrus.database.engine.OperationType.INDEX;
import static com.authrus.database.engine.OperationType.INSERT;
import static com.authrus.database.engine.OperationType.ROLLBACK;
import static com.authrus.database.engine.OperationType.UPDATE;

import java.io.IOException;

import com.authrus.database.engine.OperationType;
import com.authrus.database.engine.TransactionFilter;
import com.authrus.database.engine.io.DataRecord;
import com.authrus.database.engine.io.DataRecordReader;
import com.authrus.database.engine.io.write.ChangeRecordReader;

public class ChangeDispatcher {
   
   private final BatchOperationProcessor processor;
   private final TransactionFilter filter;
   private final boolean restore;
   
   public ChangeDispatcher(ChangeScheduler scheduler, TransactionFilter filter) {
      this(scheduler, filter, false);
   }
   
   public ChangeDispatcher(ChangeScheduler scheduler, TransactionFilter filter, boolean restore) {
      this.processor = new BatchOperationProcessor(scheduler);
      this.restore = restore;
      this.filter = filter;
   }
   
   public void dispatch(DataRecord record) throws IOException {
      String table = record.getName();
      DataRecordReader reader = record.getReader();
      int size = reader.readInt();
      
      if(size > 0) {     
         ChangeRecordReader builder = null;
         
         for(int i = 0; i < size; i++) {            
            char code = reader.readChar();
            String origin = reader.readString();
            OperationType type = OperationType.resolveType(code);
   
            if(type == BEGIN) {
               builder = new BeginRecordReader(filter, origin, table, restore);
            } else if(type == BATCH) {
               builder = new BatchRecordReader(filter, origin, table, restore);               
            } else if(type == CREATE) {
               builder = new CreateRecordReader(origin, table);
            } else if(type == DROP) {
               builder = new DropRecordReader(origin, table);
            } else if(type == INDEX) {
               builder = new IndexRecordReader(origin, table);
            } else if(type == UPDATE) {
               builder = new UpdateRecordReader(origin, table);
            } else if(type == INSERT) {
               builder = new InsertRecordReader(origin, table);
            } else if(type == DELETE) {
               builder = new DeleteRecordReader(origin, table);
            } else if(type == COMMIT) {
               builder = new CommitRecordReader(origin, table);
            } else if(type == ROLLBACK) {
               builder = new RollbackRecordReader(origin, table);                 
            } else {
               throw new IOException("Command code '" + code + "' from '" + table + "' is not supported");
            }
            try {
               ChangeOperation operation = builder.read(reader);
               
               if(operation != null) {
                  processor.process(operation, type);
               }
            } catch(Exception e) {
               throw new IllegalStateException("Unable to parse record from '" + table + "'", e);
            }                            
         } 
      }
   }
}
