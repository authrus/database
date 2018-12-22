
package com.authrus.database.bind.table.statement;

import static com.authrus.database.data.DataConstraint.REQUIRED;
import static com.authrus.database.function.DefaultFunction.IDENTITY;

import java.util.List;

import com.authrus.database.Column;
import com.authrus.database.Database;
import com.authrus.database.DatabaseConnection;
import com.authrus.database.PrimaryKey;
import com.authrus.database.Schema;
import com.authrus.database.bind.table.TableContext;
import com.authrus.database.data.DataConstraint;
import com.authrus.database.data.DataType;
import com.authrus.database.function.DefaultFunction;
import com.authrus.database.function.DefaultValue;

public class CreateStatement<T> {

   private final TableContext<T> context;
   private final Database database;
   private final boolean conditional;
   
   public CreateStatement(Database database, TableContext<T> context) {
      this(database, context, false);
   }
   
   public CreateStatement(Database database, TableContext<T> context, boolean conditional) {
      this.conditional = conditional;
      this.database = database;  
      this.context = context;
   }
   
   public String compile() throws Exception {
      String table = context.getName();
      Schema schema = context.getSchema();      
      PrimaryKey key = schema.getKey();
      List<String> keyColumns = key.getColumns();
      int columnCount = schema.getCount();
      int keyCount = key.getCount();
      
      if(columnCount <= 0) {
         throw new IllegalStateException("Table '" + table + "' has " + columnCount + " columns");
      }
      if(keyCount <= 0) {
         throw new IllegalStateException("Table '" + table + "' has " + keyCount + " keys");
      }
      StringBuilder builder = new StringBuilder();
      
      builder.append("create table ");
      
      if(conditional) {
         builder.append("if not exists ");
      }
      builder.append(table);
      builder.append(" (\r\n");
      
      for(int i = 0; i < columnCount; i++) {
         Column column = schema.getColumn(i);
         DataConstraint dataConstraint = column.getDataConstraint();
         DefaultValue defaultValue = column.getDefaultValue();
         DefaultFunction defaultFunction = defaultValue.getFunction();
         String defaultExpression = defaultValue.getExpression();
         DataType dataType = column.getDataType();         
         String title = column.getTitle();         
         
         if(i > 0) {
            builder.append(",\r\n");
         }
         builder.append("   ");
         builder.append(title);
         
         String match = dataType.getName();
         
         if(keyColumns.contains(title)) {
            builder.append(" ");
            builder.append(match);
            builder.append(" not null");
         } else {
            if(dataConstraint == REQUIRED) {
               builder.append(" ");
               builder.append(match);
               builder.append(" not null");
            } else {
               builder.append(" ");
               builder.append(match);
            }
         }
         if(defaultFunction != IDENTITY) {
            builder.append(" default ");
            builder.append(defaultExpression);
         }
      }
      builder.append(",\r\n   primary key (");
      
      for(int i = 0; i < keyCount; i++) {
         Column column = key.getColumn(i);
         String title = column.getTitle();
         
         if(i > 0) {
            builder.append(", ");
         }
         builder.append(title);
      }
      builder.append(")\r\n");
      builder.append(")");
      
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
