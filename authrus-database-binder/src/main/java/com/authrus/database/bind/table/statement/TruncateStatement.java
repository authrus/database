package com.authrus.database.bind.table.statement;

import java.util.concurrent.atomic.AtomicLong;

import com.authrus.database.Database;
import com.authrus.database.DatabaseConnection;
import com.authrus.database.PrimaryKey;
import com.authrus.database.Schema;
import com.authrus.database.bind.table.TableContext;

public class TruncateStatement<T> {

   private final TableContext<T> context;
   private final Database database;
   
   public TruncateStatement(Database database, TableContext<T> context) {
      this.database = database;     
      this.context = context;
   }
   
   public String compile() throws Exception {
      String table = context.getName();
      Schema schema = context.getSchema();
      PrimaryKey primaryKey = schema.getKey();
      int keyCount = primaryKey.getCount();

      if(keyCount <= 0) {
         throw new IllegalStateException("Table '" + table + "' has " + keyCount + " keys");
      }
      StringBuilder builder = new StringBuilder();
      
      builder.append("delete from ");
      builder.append(table);
      
      return builder.toString();
   }
   
   public String execute() throws Exception {
      String expression = compile();
      AtomicLong lastUpdate = context.getTimeStamp();
      DatabaseConnection connection = database.getConnection();
      
      try {         
         lastUpdate.getAndIncrement();
         connection.executeStatement(expression); 
      } finally {
         connection.closeConnection();
      }
      return expression;
   }
}
