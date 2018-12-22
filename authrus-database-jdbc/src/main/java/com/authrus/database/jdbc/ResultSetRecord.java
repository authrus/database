package com.authrus.database.jdbc;

import java.util.Date;
import java.util.Map;
import java.util.Set;

import com.authrus.database.Record;
import com.authrus.database.RecordSchema;
import com.authrus.database.data.DateParser;

public class ResultSetRecord implements Record {
   
   private final Map<String, Object> record;
   private final RecordSchema schema;
   
   public ResultSetRecord(Map<String, Object> record, RecordSchema schema) {
      this.record = record;
      this.schema = schema;
   }

   @Override
   public Set<String> getColumns() throws Exception {
      return schema.getColumns();
   }

   @Override
   public String getString(String name) throws Exception {
      Object value = record.get(name);
      
      if(value != null) {
         return String.valueOf(value);
      }
      return null;
   }      

   @Override
   public Integer getInteger(String name) throws Exception {
      String value = getString(name);
      
      if(value != null) {
         return Integer.parseInt(value);
      }
      return null;
   }

   @Override
   public Double getDouble(String name) throws Exception {
      String value = getString(name);
      
      if(value != null) {
         return Double.parseDouble(value);
      }
      return null;
   }

   @Override
   public Long getLong(String name) throws Exception {
      String value = getString(name);
      
      if(value != null) {
         return Long.parseLong(value);
      }
      return null;
   }

   @Override
   public Boolean getBoolean(String name) throws Exception {
      String value = getString(name);
      
      if(value != null) {
         return Boolean.parseBoolean(value);
      }
      return null;
   }
   
   @Override
   public Date getDate(String name) throws Exception {
      Long value = getLong(name);

      if (value != null) {
         return DateParser.toDate(value);
      }
      return null;
   }

   @Override
   public Float getFloat(String name) throws Exception {
      String value = getString(name);

      if (value != null) {
         return Float.parseFloat(value);
      }
      return null;
   }

   @Override
   public Byte getByte(String name) throws Exception {
      String value = getString(name);

      if (value != null) {
         return Byte.parseByte(value);
      }
      return null;
   }   

   @Override
   public Short getShort(String name) throws Exception {
      String value = getString(name);

      if (value != null) {
         return Short.parseShort(value);
      }
      return null;
   }   

   @Override
   public Character getCharacter(String name) throws Exception {
      String value = getString(name);

      if (value != null) {
         return value.charAt(0);
      }
      return null;
   } 
}
