package com.authrus.database.engine.io.write;

import static com.authrus.database.engine.OperationType.BATCH;
import static com.authrus.database.engine.OperationType.BEGIN;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.authrus.database.engine.OperationType;

public class BatchRecordBuilder {   
   
   private final ChangeRecordWriter begin;
   private final String owner;  
   private final String table;
   
   public BatchRecordBuilder(String owner, String table) {
      this.begin = new BatchRecordWriter(owner); 
      this.table = table;
      this.owner = owner;      
   }
   
   public ChangeRecordBatch create(OperationType type, String origin) throws IOException {      
      List<ChangeRecord> records = new ArrayList<ChangeRecord>();
      ChangeRecordBatch batch = new ChangeRecordBatch(records, origin, table);
      
      if(!type.equals(BEGIN) && !type.equals(BATCH)) {
         ChangeRecord record = new ChangeRecord(begin, BATCH, owner, table);            
      
         if(!origin.equals(owner)) {
            throw new IOException("Record from '" + origin + "' is not in a transaction");
         }
         records.add(record);
      }        
      return batch;     
   }
}
 