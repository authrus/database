package com.authrus.database;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class RecordSchema {
   
   private final List<String> columns;
   
   public RecordSchema() {
      this.columns = new ArrayList<String>();
   }
   
   public int getCount() {
      return columns.size();
   }
   
   public Set<String> getColumns() {
      Set<String> values = new LinkedHashSet<String>();
      
      for(String column : columns) {
         values.add(column);
      }
      return values;
   }
   
   public String getColumn(int index) {
      int size = columns.size();
      
      if(index >= size) {
         throw new IllegalArgumentException("Index " + index + " out of bounds for schema " + columns);
      }
      return columns.get(index);
   }
   
   public int getIndex(String name) {
      int index = columns.indexOf(name);
      
      if(index == -1) {
         throw new IllegalArgumentException("No column called '" + name + "' in schema " + columns);         
      }
      return index;
   }   
   
   public void addColumn(String name) {
      int index = columns.indexOf(name);
      
      if(index != -1) {
         throw new IllegalArgumentException("Column '" + name + "' already exists in schema " + columns);
      }
      columns.add(name);
   }
   
   @Override
   public String toString() {
      return columns.toString();
   }
}
