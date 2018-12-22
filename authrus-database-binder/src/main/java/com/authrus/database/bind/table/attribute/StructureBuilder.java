package com.authrus.database.bind.table.attribute;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Properties;

import com.authrus.database.Column;
import com.authrus.database.ColumnSeries;
import com.authrus.database.Counter;
import com.authrus.database.PrimaryKey;
import com.authrus.database.Schema;

public class StructureBuilder implements Structure {
   
   private final ColumnAllocator allocator;
   private final ColumnMerger merger;
   private final Properties properties;
   private final ColumnSeries columns;
   private final ColumnSeries keys;
   private final Counter counter;
   private final List index;
   private final Class type;
   
   public StructureBuilder(Class type, List index) {        
      this.allocator = new ColumnAllocator();
      this.merger = new ColumnMerger();
      this.properties = new Properties();
      this.columns = new ColumnSeries();
      this.keys = new ColumnSeries();
      this.counter = new Counter();
      this.index = index;
      this.type = type;
   }
   
   public Schema createSchema() {
      PrimaryKey primaryKey = new PrimaryKey(keys);
      Schema schema = new Schema(primaryKey, columns, properties);
      
      if(type != null) {
         String name = type.getName();
         Object previous = properties.put("class", name);
         
         if(previous != null) {
            throw new IllegalStateException("Attribute obstructing class " + previous);
         }         
      }
      return schema;      
   }
   
   public void addColumn(Column column) {
      Column columnCopy = allocator.createColumn(column, counter);
      String columnTitle = columnCopy.getTitle();
      
      if(index.contains(columnTitle)) {
         Column index = allocator.createColumn(column, counter, true);
         
         merger.merge(keys, index);
         merger.merge(columns, index);
         counter.next();  
      } else {
         merger.merge(columns, columnCopy);
         counter.next();
      }
   }
   
   public void addColumn(Field field) {
      Column column = allocator.createColumn(field, counter);
      String columnTitle = column.getTitle();
      
      if(index.contains(columnTitle)) {
         Column index = allocator.createColumn(field, counter, true);
         
         merger.merge(keys, index);
         merger.merge(columns, index);
         counter.next();  
      } else {
         merger.merge(columns, column);
         counter.next();
      }
   }
   
   @Override
   public Structure addChild(Field field) {
      String fieldName = field.getName();
      Class fieldType = field.getType();
      String typeName = fieldType.getName();
      Object previousValue = properties.put(fieldName + ".class", typeName);
      
      if(previousValue != null) {
         throw new IllegalStateException("Structure is performing a sycle at " + field);
      } 
      return new SectionBuilder(fieldName + ".");
   }
   
   private class SectionBuilder implements Structure {         
      
      private final ColumnAllocator allocator;
      private final String prefix;
      
      public SectionBuilder(String prefix) {         
         this.allocator = new ColumnAllocator(prefix);
         this.prefix = prefix;
      }
      
      public void addColumn(Column column) {
         Column columnCopy = allocator.createColumn(column, counter);
         String columnTitle = columnCopy.getTitle();
         
         if(index.contains(columnTitle)) {
            merger.merge(keys, columnCopy);
         }
         merger.merge(columns, columnCopy);
         counter.next();  
      }
      
      public void addColumn(Field field) {
         Column column = allocator.createColumn(field, counter);
         String columnTitle = column.getTitle();
         
         if(index.contains(columnTitle)) {
            merger.merge(keys, column);
         }
         merger.merge(columns, column);
         counter.next();  
      }

      @Override
      public Structure addChild(Field field) {
         String name = field.getName();
         Class type = field.getType();
         String value = type.getName();         
         Object previous = properties.put(prefix + name + ".class", value);
         
         if(previous != null) {
            throw new IllegalStateException("Structure is performing a sycle at " + field);
         }         
         return new SectionBuilder(prefix + name + ".");
      }
   }
}
