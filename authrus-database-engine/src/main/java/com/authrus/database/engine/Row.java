package com.authrus.database.engine;

import java.util.Arrays;

public class Row {

   private final Cell[] cells;
   private final String key;
   
   public Row(String key, Cell[] cells) {
      this.cells = cells;
      this.key = key;
   }
   
   public String getKey() {
      return key;
   }
   
   public Cell getCell(int index) {
      if(index >= cells.length) {
         throw new IllegalStateException("Index out of bounds " + index);
      }
      if(index < 0) {
         throw new IllegalStateException("Index out of bounds " + index);
      }
      return cells[index];
   }
   
   public int getCount() {
      return cells.length;
   }
   
   @Override
   public String toString() {
      return Arrays.toString(cells);
   }
}
