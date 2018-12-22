package com.authrus.database.engine.io.read;

import java.io.IOException;

import com.authrus.database.engine.io.DataRecordReader;
import com.authrus.database.engine.io.write.ChangeRecordReader;

public class RollbackRecordReader implements ChangeRecordReader{
   
   private final String origin;
   private final String table;
   
   public RollbackRecordReader(String origin, String table) {
      this.origin = origin;
      this.table = table;
   }    
   
   @Override
   public ChangeOperation read(DataRecordReader reader) throws IOException {         
      return new RollbackOperation(origin, table);
   }
}
