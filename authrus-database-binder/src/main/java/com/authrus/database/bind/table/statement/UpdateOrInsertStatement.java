package com.authrus.database.bind.table.statement;

import com.authrus.database.Column;
import com.authrus.database.Database;
import com.authrus.database.DatabaseConnection;
import com.authrus.database.PrimaryKey;
import com.authrus.database.Record;
import com.authrus.database.ResultIterator;
import com.authrus.database.Schema;
import com.authrus.database.Statement;
import com.authrus.database.bind.table.TableContext;
import com.authrus.database.data.DataType;

public class UpdateOrInsertStatement<T> {

   private final InsertStatement<T> insert;
   private final UpdateStatement<T> update;
   private final TableContext<T> context;
   private final Database database;
   
   public UpdateOrInsertStatement(Database database, TableContext<T> context) {
      this.update = new UpdateStatement(database, context);
      this.insert = new InsertStatement(database, context);
      this.database = database;     
      this.context = context;
   }

   public String compile() throws Exception {
      String table = context.getName();
      Schema schema = context.getSchema();
      PrimaryKey primaryKey = schema.getKey();
      int columnCount = schema.getCount();
      int keyCount = primaryKey.getCount();
      
      if(columnCount <= 0) {
         throw new IllegalStateException("Table '" + table + "' has " + columnCount + " updatable columns");
      }
      if(keyCount <= 0) {
         throw new IllegalStateException("Table '" + table + "' has " + keyCount + " keys");
      }
      StringBuilder builder = new StringBuilder();
      
      builder.append("select count(*) from ");
      builder.append(table);
      builder.append(" where ");
      
      for(int i = 0; i < keyCount; i++) {
         Column column = primaryKey.getColumn(i);
         String columnTitle = column.getTitle();
         
         if(i > 0) {
            builder.append(" and ");
         }
         builder.append(columnTitle);
         builder.append(" == :");
         builder.append(columnTitle);
      }
      return builder.toString();         
   }
   
   public String execute(T object) throws Exception {
      String table = context.getName();
      Schema schema = context.getSchema();
      RecordMapper mapper = context.getMapper();
      Record record = mapper.fromObject(object);
      PrimaryKey primaryKey = schema.getKey();
      int keyCount = primaryKey.getCount();
      
      if(keyCount <= 0) {
         throw new IllegalStateException("Table '" + table + "' has " + keyCount + " keys");
      }
      String expression = compile();
      DatabaseConnection connection = database.getConnection();
      int matches = 0;
      
      try {
         Statement statement = connection.prepareStatement(expression);
         
         for(int i = 0; i < keyCount; i++) {
            Column column = schema.getColumn(i);
            String columnTitle = column.getTitle();
            String columnName = column.getName();
            DataType dataType = column.getDataType();
            Comparable value = dataType.getData(record, columnName);         
           
            if(value != null) {
               dataType.setData(statement, columnTitle, value);
            }
         }
         ResultIterator<Record> iterator = statement.execute();
         
         if(!iterator.isEmpty()) {
            Record result = iterator.next();
           
            if(result != null) {
               matches = result.getInteger("count(*)");
            }
         }
      } finally {
         connection.closeConnection();
      }            
      if(matches <= 0) {
         return insert.execute(object);
      }
      return update.execute(object); 
   }
}
