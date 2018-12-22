package com.authrus.database.engine.io;

public class DataRecordFilter {
   
   private final DataRecordConsumer consumer;
   private final String filter;
   
   public DataRecordFilter(DataRecordConsumer consumer) {
      this(consumer, null);
   }
   
   public DataRecordFilter(DataRecordConsumer consumer, String filter) {
      this.consumer = consumer;
      this.filter = filter;
   }
  
   public DataRecord read() {
      DataRecord record = consumer.read();
      
      if(record != null) {
         if(filter == null) {
            return record;
         }
         while(record != null) {
            String source = record.getName();
         
            if(filter.equals(source)) {
               return record;
            }
            record = consumer.read(); 
         }
      }            
      return null;
   }
}
