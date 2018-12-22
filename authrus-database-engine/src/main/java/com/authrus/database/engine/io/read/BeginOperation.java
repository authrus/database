package com.authrus.database.engine.io.read;

import com.authrus.database.engine.Transaction;
import com.authrus.database.engine.TransactionFilter;

public class BeginOperation implements ChangeOperation {
   
   private final TransactionFilter filter;
   private final Transaction transaction; 

   public BeginOperation(TransactionFilter filter, Transaction transaction) {
      this.transaction = transaction;
      this.filter = filter;      
   }
   
   @Override
   public boolean execute(ChangeAssembler assembler) {
      String origin = transaction.getOrigin();
      String table = transaction.getTable();
      
      if(filter.accept(transaction)) {
         assembler.onBegin(origin, table, transaction);         
         return true;
      } 
      return false;
   }
}
