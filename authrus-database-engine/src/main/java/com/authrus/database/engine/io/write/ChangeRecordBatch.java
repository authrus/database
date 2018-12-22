package com.authrus.database.engine.io.write;

import java.util.List;

public class ChangeRecordBatch { 

   private final List<ChangeRecord> records;  
   private final String origin;
   private final String table;

   public ChangeRecordBatch(List<ChangeRecord> records, String origin, String table) {
      this.records = records;
      this.origin = origin;
      this.table = table;
   }
   
   public List<ChangeRecord> getRecords() {
      return records;
   }     
   
   public String getOrigin() {
      return origin;
   }
   
   public String getTable() {
      return table;
   }   
}
