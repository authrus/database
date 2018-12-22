package com.authrus.database.engine.io.read;

import com.authrus.database.Schema;

public class CreateOperation implements ChangeOperation {
   
   private final Schema schema;     
   private final String origin;
   private final String table;

   public CreateOperation(String origin, String table, Schema schema) {
      this.origin = origin;
      this.schema = schema;        
      this.table = table;
   }
   
   @Override
   public boolean execute(ChangeAssembler assembler) {         
      assembler.onCreate(origin, table, schema);
      return true;
   }
}