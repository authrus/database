package com.authrus.database.engine.io.read;


public class UpdateOperation implements ChangeOperation {
   
   private final ChangeSet change;     
   private final String origin;
   private final String table;

   public UpdateOperation(String origin, String table, ChangeSet change) {
      this.change = change;
      this.origin = origin;
      this.table = table;
   }
   
   @Override
   public boolean execute(ChangeAssembler assembler) {         
      assembler.onUpdate(origin, table, change);
      return true;
   }
}