package com.authrus.database.engine.io.read;

import java.io.IOException;

import com.authrus.database.engine.io.DataRecordReader;
import com.authrus.database.engine.io.write.ChangeRecordReader;

public class IndexRecordReader implements ChangeRecordReader{
   
   private final String origin;
   private final String table;
   
   public IndexRecordReader(String origin, String table) {     
      this.origin = origin;
      this.table = table;
   } 
   
   @Override
   public ChangeOperation read(DataRecordReader reader) throws IOException {
      String column = reader.readString();
         
      if(column == null) {
         throw new IllegalStateException("Index statement for '" + table + "' does not have a column");
      }
      return new IndexOperation(origin, table, column);
   }
}
