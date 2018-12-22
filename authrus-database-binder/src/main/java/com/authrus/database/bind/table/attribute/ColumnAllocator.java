package com.authrus.database.bind.table.attribute;

import static com.authrus.database.data.DataConstraint.OPTIONAL;
import static com.authrus.database.data.DataConstraint.REQUIRED;

import java.lang.reflect.Field;

import com.authrus.database.Column;
import com.authrus.database.Counter;
import com.authrus.database.data.DataConstraint;
import com.authrus.database.data.DataType;
import com.authrus.database.function.DefaultValue;

public class ColumnAllocator {
   
   private final String prefix;
   
   public ColumnAllocator() {
      this(null);
   }
   
   public ColumnAllocator(String prefix) {
      this.prefix = prefix;
   }

   public Column createColumn(Column column, Counter counter) {
      return createColumn(column, counter, false);
   }
   
   public Column createColumn(Column column, Counter counter, boolean required) {
      DataConstraint constraint = required ? REQUIRED : OPTIONAL;
      DefaultValue defaultValue = column.getDefaultValue();
      String defaultExpression = defaultValue.getExpression();
      String columnName = column.getName();
      DataType dataType = column.getDataType();
      int index = counter.get();
      
      if(prefix != null) {
         String fieldPath = prefix.concat(columnName);
         String columnTitle = createName(fieldPath);
      
         return new Column(constraint, dataType, defaultExpression, fieldPath, columnTitle, index);
      }
      return new Column(constraint, dataType, defaultExpression, columnName, columnName, index);
   }
   
   public Column createColumn(Field field, Counter counter) {
      return createColumn(field, counter, false);
   }
   
   public Column createColumn(Field field, Counter counter, boolean required) {
      DataConstraint constraint = required ? REQUIRED : OPTIONAL;
      String fieldName = field.getName();
      Class fieldType = field.getType();
      DataType dataType = DataType.resolveType(fieldType);
      int index = counter.get();
      
      if(prefix != null) {
         String fieldPath = prefix.concat(fieldName);
         String columnTitle = createName(fieldPath);
      
         return new Column(constraint, dataType, null, fieldPath, columnTitle, index);
      }        
      return new Column(constraint, dataType, null, fieldName, fieldName, index);
   }   
   
   private String createName(String path) {
      int index = path.indexOf('.');
      
      if(index != -1) {
         String[] parts = path.split("\\.");
         
         if(parts.length > 0) {
            StringBuilder builder = new StringBuilder();
            
            for(int i = 0; i < parts.length; i++) {
               String part = parts[i];
               
               if(i > 0) {
                  String start = part.substring(0, 1);
                  String upperCase = start.toUpperCase();
                  String suffix = part.substring(1);
               
                  builder.append(upperCase);
                  builder.append(suffix);
               } else {
                  builder.append(part);
               }            
            }
            return builder.toString();
         }
      }
      return path;
   }
}
