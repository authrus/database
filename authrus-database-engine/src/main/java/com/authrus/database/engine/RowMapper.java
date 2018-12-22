package com.authrus.database.engine;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.authrus.database.Column;
import com.authrus.database.PrimaryKey;
import com.authrus.database.Schema;
import com.authrus.database.attribute.AttributeSerializer;
import com.authrus.database.attribute.AttributeWriter;
import com.authrus.database.attribute.MapWriter;

public class RowMapper {
   
   private final AttributeSerializer serializer;
   private final Schema schema;
   
   public RowMapper(AttributeSerializer serializer, Schema schema) {
      this.serializer = serializer;
      this.schema = schema;
   }

   public Row createTuple(Object object) throws Exception {
      Map<String, Object> message = new LinkedHashMap<String, Object>();
      AttributeWriter writer = new MapWriter(message);
      
      if(object == null) {
         throw new IllegalStateException("Cannot create a row from null");
      }
      PrimaryKey key = schema.getKey();
      List<String> columns = key.getColumns();
      int count = schema.getCount();
      
      if(count == 0) {
         throw new IllegalStateException("Table schema contains no columns");
      }
      StringBuilder builder = new StringBuilder();
      Cell[] cells = new Cell[count];
      
      serializer.write(object, writer);      
   
      for(int i = 0; i < count; i++) {
         Column column = schema.getColumn(i);
         String name = column.getName();
         Object value = message.get(name);
         
         cells[i] = new Cell(column, (Comparable)value);
      }
      for(String column : columns) {
         Object value = message.get(column);
         
         if(value == null) {
            throw new IllegalStateException("Key column '" + column + "' was null");
         }
         builder.append(value);            
      }
      String result = builder.toString();
      int length = result.length();
      
      if(length == 0) {
         throw new IllegalStateException("Not enough data to build a row key");
      }
      return new Row(result, cells);
   }
}
