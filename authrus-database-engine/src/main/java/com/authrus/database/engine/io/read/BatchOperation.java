package com.authrus.database.engine.io.read;

import java.util.List;

public class BatchOperation implements ChangeOperation {
   
   private final List<ChangeOperation> operations;

   public BatchOperation(List<ChangeOperation> operations) {
      this.operations = operations;
   }
   
   @Override
   public boolean execute(ChangeAssembler assembler) {        
      for(ChangeOperation operation : operations) {
         if(!operation.execute(assembler)){
            return false;
         }
      }
      return true;
   }

}
