package com.authrus.database.terminal.console;

import java.util.Arrays;
import java.util.List;

public class ConsoleRow {
   
   private final List<String> indexes;
   private final String[] values;
   
   public ConsoleRow(String[] names, String[] values) {
      this.indexes = Arrays.asList(names);
      this.values = values;
   }
   
   public void set(String name, Object value) {
      int index = indexes.indexOf(name);
      
      if(index == -1) {
         throw new IllegalArgumentException("Column '" + name + "' does not exist");
      } 
      if(value == null) {
         values[index] = "NULL"; // big fat null
      } else {
         values[index] = String.valueOf(value);
      }  
   }
   
   public void set(int index, Object value) {
      int width = values.length;
      
      if(index >= width) {
         throw new IllegalArgumentException("Index is " + index + " but there are " + width + " columns");
      } 
      if(value == null) {
         values[index] = "NULL"; // big fat null
      } else {
         values[index] = String.valueOf(value);
      }  
   }

}
