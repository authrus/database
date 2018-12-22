package com.authrus.database.engine.io.read;

import java.util.Iterator;

import com.authrus.database.engine.TransactionFilter;
import com.authrus.database.engine.io.DataRecord;

public class ChangeProcessor {   
   
   private final ChangeDispatcher reader;

   public ChangeProcessor(ChangeScheduler scheduler, TransactionFilter filter) {
      this(scheduler, filter, false);
   }
   
   public ChangeProcessor(ChangeScheduler scheduler, TransactionFilter filter, boolean restore) {
      this.reader = new ChangeDispatcher(scheduler, filter, restore);
   }

   public int process(Iterator<DataRecord> iterator) throws Exception {
      int count = 0;
      
      while(iterator.hasNext()) {
         DataRecord record = iterator.next();
         String table = record.getName();
         
         try {
            reader.dispatch(record);          
         } catch(Exception e) {
            throw new IllegalStateException("Error processing record from '" + table + "'", e);
         }
         count++;                  
      }
      return count;
   }
}
