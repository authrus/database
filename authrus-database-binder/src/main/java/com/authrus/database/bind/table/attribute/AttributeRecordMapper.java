package com.authrus.database.bind.table.attribute;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.authrus.database.Column;
import com.authrus.database.Record;
import com.authrus.database.Schema;
import com.authrus.database.attribute.AttributeSerializer;
import com.authrus.database.attribute.MapReader;
import com.authrus.database.attribute.MapWriter;
import com.authrus.database.bind.table.statement.RecordMapper;
import com.authrus.database.data.DataType;
import com.authrus.database.function.DefaultValue;

public class AttributeRecordMapper<T> implements RecordMapper<T> {
   
   private final AttributeSerializer serializer;
   private final Schema schema;
   
   public AttributeRecordMapper(AttributeSerializer serializer, Schema schema) {
      this.serializer = serializer;      
      this.schema = schema;
   }

   @Override
   public Record fromObject(T object) throws Exception {
      int count = schema.getCount();
      
      if(count == 0) {
         throw new IllegalStateException("Schema contains no columns"); 
      }
      Map<String, Object> row = new HashMap<String, Object>();
      MapWriter writer = new MapWriter(row);

      serializer.write(object, writer);
      
      for(int i = 0; i < count; i++) {
         Column column = schema.getColumn(i);
         DefaultValue defaultValue = column.getDefaultValue();
         String columnName = column.getName();
         String columnTitle = column.getTitle();
         Object columnValue = row.get(columnName);
         
         if(columnValue != null) {
            Comparable originalValue = (Comparable)columnValue;
            Comparable resultValue = defaultValue.getDefault(column, originalValue);
            
            if(originalValue != resultValue) {
               row.put(columnTitle, resultValue);
            }
         }
      }
      return new AttributeRecord(row);
   }

   @Override
   public T toObject(Record record) throws Exception {
      Properties properties = schema.getProperties();      
      int count = schema.getCount();
      
      if(count == 0) {
         throw new IllegalStateException("Schema contains no columns"); 
      }
      Map<String, Object> values = new LinkedHashMap<String, Object>();
      MapReader reader = new MapReader(values);
      
      for(int i = 0; i < count; i++) {
         Column column = schema.getColumn(i);
         String columnTitle = column.getTitle();
         String columnName = column.getName();
         DataType dataType = column.getDataType();
         Object recordValue = dataType.getData(record, columnTitle);
         
         if(recordValue != null) {
            values.put(columnName, recordValue);
         }
      }
      Set<String> keys = properties.stringPropertyNames();
      
      for(String key : keys) {
         String value = properties.getProperty(key);
         
         if(value != null) {
            values.put(key, value);
         }
      }
      return (T)serializer.read(reader);
   }

}
