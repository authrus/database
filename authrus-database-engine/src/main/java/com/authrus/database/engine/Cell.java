package com.authrus.database.engine;

import com.authrus.database.Column;

public class Cell {

   private final Comparable value;
   private final Column column;
   
   public Cell(Column column, Comparable value) {
      this.column = column;
      this.value = value;  
   }
   
   public Comparable getValue(){
      return value;
   }
   
   public Column getColumn() {
      return column;
   }
   
   @Override
   public String toString() {
      return String.valueOf(value);
   }
}
