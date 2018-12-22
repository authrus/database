package com.authrus.database.engine.io.read;


public class RollbackOperation implements ChangeOperation {
   
   private final String origin;
   private final String table;

   public RollbackOperation(String origin, String table) {
      this.origin = origin;
      this.table = table;
   }
   
   @Override
   public boolean execute(ChangeAssembler assembler) {
      assembler.onRollback(origin, table);      
      return true;
   }
}