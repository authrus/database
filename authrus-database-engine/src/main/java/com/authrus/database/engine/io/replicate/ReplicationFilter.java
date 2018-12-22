package com.authrus.database.engine.io.replicate;

import com.authrus.database.engine.Transaction;
import com.authrus.database.engine.TransactionFilter;

public class ReplicationFilter implements TransactionFilter {
   
   private final PositionRecorder recorder;
   private final String owner;
   
   public ReplicationFilter(Position position, String owner) {
      this.recorder = new PositionRecorder(position, owner);
      this.owner = owner;
   }

   @Override
   public boolean accept(Transaction transaction) {      
      String origin = transaction.getOrigin();
      
      if(origin.equals(owner)) {
         return false;
      }  
      if(!recorder.accept(transaction)) {
         return false;
      }   
      return true;
   }

}
