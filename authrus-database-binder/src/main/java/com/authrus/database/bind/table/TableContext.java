package com.authrus.database.bind.table;

import java.util.concurrent.atomic.AtomicLong;

import com.authrus.database.PrimaryKey;
import com.authrus.database.Schema;
import com.authrus.database.bind.table.statement.RecordMapper;
import com.authrus.database.data.DataConverter;

public class TableContext<T> {

   private final DataConverter converter;
   private final RecordMapper<T> mapper;
   private final AtomicLong update;
   private final Schema schema;
   private final Class<T> type;
   private final String name;

   public TableContext(Schema schema, RecordMapper<T> mapper, Class type, String name) {
      this.converter = new DataConverter();
      this.update = new AtomicLong();
      this.mapper = mapper;
      this.schema = schema;
      this.type = type;
      this.name = name;
   }
   
   public DataConverter getConverter() {
      return converter;
   }
   
   public AtomicLong getTimeStamp() {
      return update;
   }
   
   public PrimaryKey getPrimaryKey() {
      return schema.getKey();
   }
   
   public Schema getSchema() {
      return schema;
   }
   
   public RecordMapper<T> getMapper() {
      return mapper;
   }
   
   public Class<T> getType() {
      return type;
   }   
   
   public String getName() {
      return name;
   }
}
