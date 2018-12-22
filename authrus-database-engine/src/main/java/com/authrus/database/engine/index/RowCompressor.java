package com.authrus.database.engine.index;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.authrus.database.Column;
import com.authrus.database.common.collection.LeastRecentlyUsedMap;
import com.authrus.database.data.DataType;
import com.authrus.database.engine.Cell;
import com.authrus.database.engine.Row;
import com.authrus.database.function.DefaultFunction;
import com.authrus.database.function.DefaultValue;

public class RowCompressor {

   private final Map<Comparable, Comparable> duplicates;
   private final AtomicLong sequence;
   private final int capacity;

   public RowCompressor() {
      this(10000);
   }
   
   public RowCompressor(int capacity) {
      this.duplicates = new LeastRecentlyUsedMap<Comparable, Comparable>(capacity);
      this.sequence = new AtomicLong();
      this.capacity = capacity;
   }
   
   public Row compress(Row row) { // this may slow down insert performance
      int count = row.getCount();

      if(capacity > 0 && count > 0) {
         for(int i = 0; i < count; i++) {
            Cell intern = intern(row, i);
            
            if(intern != null) {
               Cell[] cells = new Cell[count];
               
               for(int j = 0; j < i; j++) {
                  cells[j] = row.getCell(j);
               }
               cells[i] = intern;
               
               return replace(row, cells, i + 1);
            }                            
         }
      }
      return row;
   }
   
   private Row replace(Row row, Cell[] cells, int begin) {
      String key = row.getKey();
      int count = cells.length;
      
      for(int i = begin; i < count; i++) {     
         Cell intern = intern(row, i);
         
         if(intern == null) {
            cells[i] = row.getCell(i);
         } else {
            cells[i] = intern;
         }
      }
      return new Row(key, cells);
   }
   
   private Cell intern(Row row, int index) {
      Cell cell = row.getCell(index);
      Column column = cell.getColumn();
      DataType type = column.getDataType();
      DefaultValue defaults = column.getDefaultValue();
      DefaultFunction function = defaults.getFunction();
      
      if(type == DataType.SYMBOL) {
         Comparable value = cell.getValue();
         
         if(value != null) {
            Comparable result = duplicates.get(value);
            
            if(result == null) {
               duplicates.put(value, value);
               return null;
            } 
            if(result != value) {
               return new Cell(column, result);
            }
         }
      }
      if(function == DefaultFunction.SEQUENCE) {
         Comparable value = cell.getValue();
         Comparable next = defaults.getDefault(column, null);         

         if(next != value) {
            return new Cell(column, next);
         }
      }
      return null;      
   }   
   
   public void reset() {
      duplicates.clear();;
   }   
}
