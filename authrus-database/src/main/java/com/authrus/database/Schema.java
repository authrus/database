package com.authrus.database;

import java.util.List;
import java.util.Properties;

public class Schema {
   
   private final Properties properties;
   private final ColumnSeries columns;
   private final PrimaryKey key;
   
   public Schema(PrimaryKey key, ColumnSeries columns, Properties properties) {
      this.properties = properties;
      this.columns = columns;
      this.key = key;
   }
   
   public int getCount() {
      return columns.getCount();
   }
   
   public PrimaryKey getKey() {
      return key;
   }
   
   public List<String> getColumns() {
      return columns.getColumns();
   }
   
   public Column getColumn(int index) {
      return columns.getColumn(index);
   }
   
   public Column getColumn(String name) {
      return columns.getColumn(name);
   }
   
   public Properties getProperties() {
      return properties;
   } 
   
   @Override
   public String toString() {
      return columns.toString();
   }
}

