package com.authrus.database;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ColumnSeries {
   
   private final Map<String, Column> columns;
   private final List<String> names;
   
   public ColumnSeries() {
      this.columns = new LinkedHashMap<String, Column>();
      this.names = new ArrayList<String>();
   }
   
   public int getCount() {
      return names.size();
   }
   
   public List<String> getColumns() {
      List<String> columns = new LinkedList<String>();
      
      for(String name : names) {
         columns.add(name);
      }
      return columns;
   }
   
   public Column getColumn(int index) {
      int size = names.size();
      
      if(index >= size) {
         throw new IllegalArgumentException("Index " + index + " out of bounds " + names);
      }
      String name = names.get(index);
      
      return columns.get(name);
   }
   
   public Column getColumn(String name) {
      Column column = columns.get(name);
      
      if(column == null) {
         throw new IllegalArgumentException("Column '" + name + "' not found in " + names);
      }
      return column;
   }
   
   public void addColumn(Column column) {
      String title = column.getTitle();      

      if(!names.contains(title)) {
         names.add(title);
      }
      columns.put(title, column);      
   }
   
   public boolean isEmpty() {
      return names.isEmpty();
   }
   
   @Override
   public String toString() {
      return names.toString();
   }
}
