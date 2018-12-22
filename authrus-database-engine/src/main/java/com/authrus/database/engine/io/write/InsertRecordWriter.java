package com.authrus.database.engine.io.write;

import static com.authrus.database.engine.OperationType.INSERT;

import java.io.IOException;

import com.authrus.database.engine.Cell;
import com.authrus.database.engine.Row;
import com.authrus.database.engine.io.DataRecordCounter;
import com.authrus.database.engine.io.DataRecordWriter;

public class InsertRecordWriter implements ChangeRecordWriter {
   
   private final Row tuple;
   private final String origin;
   
   public InsertRecordWriter(String origin, Row tuple) {
      this.origin = origin;     
      this.tuple = tuple;
   }

   @Override
   public void write(DataRecordWriter writer, DataRecordCounter counter) throws IOException {
      if(tuple == null) {
         throw new IllegalStateException("Insert does not have a valid row");
      }
      String key = tuple.getKey();
      int count = tuple.getCount();
      
      writer.writeChar(INSERT.code);
      writer.writeString(origin);
      writer.writeString(key);
      writer.writeInt(count);
      
      for(int i = 0; i < count; i++) {
         Cell cell = tuple.getCell(i);
         Comparable value = cell.getValue();
         
         writer.writeInt(i);
         writer.writeValue(value);
      }
   }
}
