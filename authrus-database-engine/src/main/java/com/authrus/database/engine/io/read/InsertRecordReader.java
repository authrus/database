package com.authrus.database.engine.io.read;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.authrus.database.engine.io.DataRecordReader;
import com.authrus.database.engine.io.write.ChangeRecordReader;

public class InsertRecordReader implements ChangeRecordReader{
   
   private final String origin;
   private final String table;
   
   public InsertRecordReader(String origin, String table) {
      this.origin = origin;
      this.table = table;
   }    
   
   @Override
   public ChangeOperation read(DataRecordReader reader) throws IOException {
      String key = reader.readString();
      int count = reader.readInt();
         
      if(count == 0) {
         throw new IllegalStateException("Insert statement for '" + table + "' has no columns");
      }
      Map<Integer, Comparable> attributes = new HashMap<Integer, Comparable>();
      ChangeSet change = new ChangeSet(attributes, key);
      
      for(int i = 0; i < count; i++) {
         Integer index = reader.readInt();
         Comparable value = reader.readValue(); 
 
         attributes.put(index, value);                       
      }
      return new InsertOperation(origin, table, change);
   }
}
