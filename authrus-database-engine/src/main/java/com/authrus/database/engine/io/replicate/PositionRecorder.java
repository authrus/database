package com.authrus.database.engine.io.replicate;

import com.authrus.database.engine.Transaction;
import com.authrus.database.engine.TransactionFilter;

public class PositionRecorder implements TransactionFilter {
   
   private final Position position;
   private final String owner;
   
   public PositionRecorder(Position position, String owner) {
      this.position = position;     
      this.owner = owner;
   }

   @Override
   public boolean accept(Transaction transaction) {      
      String origin = transaction.getOrigin();
      String table = transaction.getTable();
      long time = transaction.getTime();
      long count = transaction.getSequence();
      
      if(!origin.equals(owner)) {
         long currentTime = position.getTime(table);
         
         if(time < currentTime) {
            return false;
         }
         if(time == currentTime) {
            long currentCount = position.getCount(table);
            
            if(count <= currentCount) {              
               return false;
            }
         }
         position.setTime(table, time);
         position.setCount(table, count);
      }
      return true;
   }
}
