package com.authrus.database.engine.io.read;


public class DropOperation implements ChangeOperation {
   
   private final String origin;
   private final String table;

   public DropOperation(String origin, String table) {
      this.origin = origin;
      this.table = table;
   }
   
   @Override
   public boolean execute(ChangeAssembler assembler) {        
      assembler.onDrop(origin, table);
      return true;
   }
}