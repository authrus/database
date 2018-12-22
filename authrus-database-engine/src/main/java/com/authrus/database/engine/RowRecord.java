package com.authrus.database.engine;

import java.util.Collections;
import java.util.Date;
import java.util.Set;

import com.authrus.database.Column;
import com.authrus.database.Record;
import com.authrus.database.Schema;
import com.authrus.database.data.DateParser;

public class RowRecord implements Record {

   private final Schema schema;
   private final Row tuple;
   private final Set<String> names;

   public RowRecord(Schema schema, Row tuple, Set<String> names) {
      this.names = Collections.unmodifiableSet(names);
      this.schema = schema;
      this.tuple = tuple;
   }

   @Override
   public Set<String> getColumns() throws Exception {
      return names;
   }   

   @Override
   public String getString(String name) throws Exception {
      Column column = schema.getColumn(name);
      int index = column.getIndex();
      Cell cell = tuple.getCell(index);
      Object value = cell.getValue();

      if (value != null) {
         return String.valueOf(value);
      }
      return null;
   }

   @Override
   public Integer getInteger(String name) throws Exception {
      String value = getString(name);

      if (value != null) {
         return Integer.parseInt(value);
      }
      return null;
   }

   @Override
   public Double getDouble(String name) throws Exception {
      String value = getString(name);

      if (value != null) {
         return Double.parseDouble(value);
      }
      return null;
   }

   @Override
   public Long getLong(String name) throws Exception {
      String value = getString(name);

      if (value != null) {
         return Long.parseLong(value);
      }
      return null;
   }

   @Override
   public Boolean getBoolean(String name) throws Exception {
      String value = getString(name);

      if (value != null) {
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
