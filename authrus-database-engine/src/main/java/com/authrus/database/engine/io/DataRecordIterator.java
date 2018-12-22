package com.authrus.database.engine.io;

import java.util.Iterator;

public class DataRecordIterator implements Iterator<DataRecord> {
   
   private DataRecordFilter filter;
   private DataRecord next;
   
   public DataRecordIterator(DataRecordConsumer consumer) {
      this(consumer, null);
   }
   
   public DataRecordIterator(DataRecordConsumer consumer, String filter) {
      this.filter = new DataRecordFilter(consumer, filter);
   }

   @Override
   public boolean hasNext() {
      if(filter == null) {
         return false;
      }
      if(next == null) {
         next = filter.read();
         
         if(next == null) {
            filter = null;
            return false;
         }
      }
      return true;
   }

   @Override
   public DataRecord next() {
      DataRecord result = next;
      
      if(result == null) {
         if(hasNext()) {
            result = next;
         }
      }
      if(result != null) {
         next = null;
      }
      return result;
   }

   @Override
   public void remove() {
      throw new UnsupportedOperationException("Remove is not supported");
   }

}
