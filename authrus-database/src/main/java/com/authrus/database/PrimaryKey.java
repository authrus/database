package com.authrus.database;

import java.util.List;

public class PrimaryKey {
   
   private final ColumnSeries keys;
   
   public PrimaryKey(ColumnSeries keys) {
      this.keys = keys;
   }
   
   public int getCount() {
      return keys.getCount();
   }
   
   public Column getColumn(int index) {
      return keys.getColumn(index);
   }
   
   public Column getColumn(String name) {
      return keys.getColumn(name);
   }
   
   public List<String> getColumns() {
      return keys.getColumns();
   }
   
   @Override
   public String toString(){
      return keys.toString();
   }
}
