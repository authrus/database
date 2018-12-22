package com.authrus.database.engine;

import com.authrus.database.Column;
import com.authrus.database.PrimaryKey;
import com.authrus.database.Schema;

public class TableBuilder {

   private final TransactionManager manager;
   private final ChangeListener listener;
   private final String owner;
   
   public TableBuilder(TransactionManager manager, ChangeListener listener, String owner) {
      this.listener = listener;
      this.manager = manager;
      this.owner = owner;
   }    
   
   public Table createTable(String origin, String name, Schema schema) {
      PrimaryKey key = schema.getKey();
      int count = key.getCount();
      
      if(count <= 0) {
         throw new IllegalStateException("No key columns defined for '" + name + "'");
      }
      TableModel model = createModel(origin, name, schema);

      for(int i = 0; i < count; i++) {  
         Column column = key.getColumn(i);
         
         if(column != null) {
            model.index(column); // ignore key index
         }
      }
      return new Table(schema, model, name);
   }   
   
   private TableModel createModel(String origin, String name, Schema schema) {
      PrimaryKey key = schema.getKey();
      int count = key.getCount();
      
      if(count <= 0) {
         throw new IllegalStateException("No key columns defined for '" + name + "'");
      }
      return new TableModel(manager, listener, key, owner, name);         
   }   
}
