package com.authrus.database.engine.io.read;


public class DeleteOperation implements ChangeOperation {
   
   private final String origin;
   private final String table;
   private final String key;

   public DeleteOperation(String origin, String table, String key) {
      this.origin = origin;
      this.table = table;
      this.key = key;
   }
   
   @Override
   public boolean execute(ChangeAssembler assembler) {        
      assembler.onDelete(origin, table, key);
      return true;
   }
}