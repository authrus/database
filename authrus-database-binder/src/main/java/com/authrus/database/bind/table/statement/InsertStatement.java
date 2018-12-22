package com.authrus.database.bind.table.statement;

import java.util.concurrent.atomic.AtomicLong;

import com.authrus.database.Column;
import com.authrus.database.Database;
import com.authrus.database.DatabaseConnection;
import com.authrus.database.Record;
import com.authrus.database.Schema;
import com.authrus.database.Statement;
import com.authrus.database.bind.table.TableContext;
import com.authrus.database.data.DataType;

public class InsertStatement<T> {

   private final TableContext<T> context;
   private final Database database;
   private final boolean conditional;

   public InsertStatement(Database database, TableContext<T> context) {
      this(database, context, false);
   }
   
   public InsertStatement(Database database, TableContext<T> context, boolean conditional) {
      this.conditional = conditional;
      this.database = database;     
      this.context = context;
   }

   public String compile() throws Exception {
      Schema schema = context.getSchema();
      String name = context.getName();
      int count = schema.getCount();
      
      if(count <= 0) {
         throw new IllegalStateException("Table '" + name + "' has " + count + " insertable columns");
      }
      StringBuilder builder = new StringBuilder();
      
      if(conditional) {
         builder.append("insert or ignore into ");
      } else {
         builder.append("insert into ");
      }      
      builder.append(name);
      builder.append(" (");
      
      for(int i = 0; i < count; i++) {
         Column column = schema.getColumn(i);
         String title = column.getTitle();
         
         if(i > 0) {
            builder.append(", ");            
         }
         builder.append(title);
      }
      builder.append(") values (");
      
      for(int i = 0; i < count; i++) {
         Column column = schema.getColumn(i);
         String title = column.getTitle();
         
         if(i > 0) {
            builder.append(", ");
         }
         builder.append(":");
         builder.append(title);         
      }
      builder.append(")");
      
      return builder.toString();          
   }
   
   public String execute(T object) throws Exception {
      String table = context.getName();
      Schema schema = context.getSchema();
      RecordMapper mapper = context.getMapper();
      AtomicLong lastUpdate = context.getTimeStamp();
      Record record = mapper.fromObject(object);
      int columnCount = schema.getCount();
      
      if(columnCount <= 0) {
         throw new IllegalStateException("Table '" + table + "' has " + columnCount + " insertable columns");
      }
      String expression = compile();
      DatabaseConnection connection = database.getConnection();
      
      try {
         Statement statement = connection.prepareStatement(expression);
         
         for(int i = 0; i < columnCount; i++) {
            Column column = schema.getColumn(i);
            String columnTitle = column.getTitle();
            String columnName = column.getName();
            DataType dataType = column.getDataType();
            Comparable value = dataType.getData(record, columnName);
            
            if(value != null) {
               dataType.setData(statement, columnTitle, value);
            }
         }
         lastUpdate.getAndIncrement();
         statement.execute();
      } finally {
         connection.closeConnection();
      }
      return expression;
   }
}
