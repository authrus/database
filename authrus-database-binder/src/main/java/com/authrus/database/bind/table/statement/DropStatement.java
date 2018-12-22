package com.authrus.database.bind.table.statement;

import com.authrus.database.Database;
import com.authrus.database.DatabaseConnection;
import com.authrus.database.Schema;
import com.authrus.database.bind.table.TableContext;

public class DropStatement<T> {

   private final TableContext<T> context;
   private final Database database;
   private final boolean conditional;

   public DropStatement(Database database, TableContext<T> context) {
      this(database, context, false);
   }
   
   public DropStatement(Database database, TableContext<T> context, boolean conditional) {
      this.conditional = conditional;
      this.database = database;   
      this.context = context;
   }
   
   public String compile() throws Exception {
      String table = context.getName();
      Schema schema = context.getSchema();
      int columnCount = schema.getCount();
      
      if(columnCount <= 0) {
         throw new IllegalStateException("Table '" + table + "' has " + columnCount + " columns");
      }
      StringBuilder builder = new StringBuilder();
      
      builder.append("drop table ");
      
      if(conditional) {
         builder.append("if exists ");
      }
      builder.append(table);
      
      return builder.toString();
   }
   
   public String execute() throws Exception {
      String statement = compile();
      DatabaseConnection connection = database.getConnection();
      
      try {
         connection.executeStatement(statement);
      } finally {
         connection.closeConnection();
      }
      return statement;
   }   
}
