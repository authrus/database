package com.authrus.database.engine.io.read;

import com.authrus.database.engine.Transaction;
import com.authrus.database.engine.TransactionFilter;
import com.authrus.database.engine.io.replicate.Position;

public class RepeatFilter implements TransactionFilter {
   
   private final Position position;
   
   public RepeatFilter() {
      this.position = new Position();
   }

   @Override
   public boolean accept(Transaction transaction) {      
      String table = transaction.getTable();
      long time = transaction.getTime();
      long count = transaction.getSequence();
      long previousTime = position.getTime(table);
      long previousCount = position.getCount(table);
      
      if(time < previousTime) {
         return false;
      }
      if(time > previousTime) {
         position.setTime(table, time);
         position.setCount(table, count);
         return true;
      }
      if(count < previousCount) {
         return false;
      }
      if(count > previousCount) {
         position.setCount(table, count);
         return true;
      }
      return false;
   }

}
