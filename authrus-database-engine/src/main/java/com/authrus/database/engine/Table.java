package com.authrus.database.engine;

import com.authrus.database.Schema;
import com.authrus.database.data.DataConverter;

public class Table {

   private final DataConverter converter;
   private final TableModel model;
   private final Schema schema;
   private final String name;
   
   public Table(Schema schema, TableModel model, String name) {
      this.converter = new DataConverter();
      this.schema = schema;
      this.model = model;      
      this.name = name;
   } 
   
   public DataConverter getConverter() {
      return converter;
   }  
   
   public TableModel getModel(){
      return model;
   }
   
   public Schema getSchema() {
      return schema;
   }  
   
   public String getName(){
      return name;
   }
}
