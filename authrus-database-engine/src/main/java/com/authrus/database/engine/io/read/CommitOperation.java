package com.authrus.database.engine.io.read;


public class CommitOperation implements ChangeOperation {
   
   private final String origin;
   private final String table;

   public CommitOperation(String origin, String table) {
      this.origin = origin;
      this.table = table;
   }
   
   @Override
   public boolean execute(ChangeAssembler assembler) {
      assembler.onCommit(origin, table);      
      return true;
   }
}