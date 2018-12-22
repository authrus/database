package com.authrus.database.engine.io.read;

import java.io.IOException;

import com.authrus.database.engine.io.DataRecordReader;
import com.authrus.database.engine.io.write.ChangeRecordReader;

public class DeleteRecordReader implements ChangeRecordReader{
   
   private final String origin;
   private final String table;
   
   public DeleteRecordReader(String origin, String table) {   
      this.origin = origin;
      this.table = table;
   }   
   
   @Override
   public ChangeOperation read(DataRecordReader reader) throws IOException {
      String key = reader.readString();
         
      if(key == null) {
         throw new IllegalStateException("Delete statement for '" + table + "' does not have a key");
      }      
      return new DeleteOperation(origin, table, key);
   }
}
