package com.authrus.database.bind.table.attribute;

import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import com.authrus.database.Record;
import com.authrus.database.data.DateParser;

public class AttributeRecord implements Record {

   private final Map<String, Object> record;
   
   public AttributeRecord(Map<String, Object> record) {
      this.record = record;
   }  
   
   @Override
   public Set<String> getColumns() throws Exception {
      Set<String> columns = record.keySet();
      
      if(columns.isEmpty()) {
         return Collections.unmodifiableSet(columns);
      }
      return Collections.emptySet();
   }     

   @Override
   public String getString(String name) throws Exception {
      return (String)record.get(name);
   }      

   @Override
   public Integer getInteger(String name) throws Exception {
      return (Integer)record.get(name);
   }

   @Override
   public Double getDouble(String name) throws Exception {
      return (Double)record.get(name);
   }

   @Override
   public Long getLong(String name) throws Exception {
      return (Long)record.get(name);
   } 

   @Override
   public Boolean getBoolean(String name) throws Exception {
      return (Boolean)record.get(name);
   }
   
   @Override
   public Float getFloat(String name) throws Exception {
      return (Float)record.get(name);
   }

   @Override
   public Byte getByte(String name) throws Exception {
      return (Byte)record.get(name);
   }   

   @Override
   public Short getShort(String name) throws Exception {
      return (Short)record.get(name);
   }   

   @Override
   public Character getCharacter(String name) throws Exception {
      return (Character)record.get(name);
   }   

   @Override
   public Date getDate(String name) throws Exception {
      Long value = getLong(name);

      if (value != null) {
         return DateParser.toDate(value);
      }
      return null;
   }
}
