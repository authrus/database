package com.authrus.database.engine.io.write;

import static com.authrus.database.engine.OperationType.UPDATE;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.authrus.database.engine.Cell;
import com.authrus.database.engine.Row;
import com.authrus.database.engine.io.DataRecordCounter;
import com.authrus.database.engine.io.DataRecordWriter;

public class UpdateRecordWriter implements ChangeRecordWriter {
   
   private final Row previous;
   private final Row current;
   private final String origin;
   
   public UpdateRecordWriter(String origin, Row current, Row previous) {
      this.previous = previous;
      this.current = current;    
      this.origin = origin;
   }
   
   @Override
   public void write(DataRecordWriter writer, DataRecordCounter counter) throws IOException {
      Map<Integer, Comparable> values = new HashMap<Integer, Comparable>();
      
      if(current == null) {
         throw new IllegalStateException("Update does not have a current row");
      }
      if(previous == null) {
         throw new IllegalStateException("Update does not have a previous row");
      }
      String key = current.getKey();
      int count = current.getCount();      
      
      writer.writeChar(UPDATE.code);
      writer.writeString(origin);
      writer.writeString(key);
      
      for(int i = 0; i < count; i++) {
         Cell currentCell = current.getCell(i);
         Cell previousCell = previous.getCell(i);
         Comparable value = currentCell.getValue(); 
            
         if(currentCell != previousCell) {
            values.put(i, value);
         }
      } 
      Set<Integer> columns = values.keySet();      
      int changes = columns.size();
      
      writer.writeInt(changes);
      
      for(Integer column : columns) {
         Comparable value = values.get(column);
         
         writer.writeInt(column);
         writer.writeValue(value);
      }
   }
}
