package com.authrus.database.engine.io.write;

import com.authrus.database.engine.OperationType;

public class ChangeRecord {
   
   private final ChangeRecordWriter writer;
   private final OperationType type;
   private final String origin;
   private final String table;
   
   public ChangeRecord(ChangeRecordWriter writer, OperationType type, String origin, String table) {
      this.writer = writer;
      this.origin = origin;
      this.table = table;
      this.type = type;
   }   
   
   public ChangeRecordWriter getWriter() {
      return writer;
   }   
   
   public OperationType getType(){
      return type;
   }
   
   public String getOrigin() {
      return origin;
   }
   
   public String getTable() {
      return table;
   }
   
   @Override
   public String toString() {
      return String.format("%s: %s -> %s", type, origin, table);
   }
}