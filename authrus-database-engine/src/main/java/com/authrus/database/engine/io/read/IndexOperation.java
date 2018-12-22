package com.authrus.database.engine.io.read;


public class IndexOperation implements ChangeOperation {
   
   private final String origin;
   private final String table;
   private final String column;

   public IndexOperation(String origin, String table, String column) {
      this.origin = origin;
      this.table = table;
      this.column = column;
   }
   
   @Override
   public boolean execute(ChangeAssembler assembler) {        
      assembler.onIndex(origin, table, column);
      return true;
   }
}