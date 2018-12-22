package com.authrus.database.engine;

import java.util.concurrent.atomic.AtomicBoolean;

import com.authrus.database.DatabaseConnection;
import com.authrus.database.Statement;
import com.authrus.database.sql.build.QueryProcessor;

public class LocalDatabaseConnection implements DatabaseConnection {
   
   private final QueryProcessor processor;
   private final AtomicBoolean disposed;
   
   public LocalDatabaseConnection(QueryProcessor processor) {
      this.disposed = new AtomicBoolean();
      this.processor = processor;      
   }  
   
   @Override
   public Statement prepareStatement(String expression) throws Exception {
      if(disposed.get()) {
         throw new IllegalStateException("Unable to execute '" + expression + "' as database connection was explicitly closed");
      }
      return prepareStatement(expression, true);
   }
   
   @Override
   public Statement prepareStatement(String expression, boolean cache) throws Exception {
      if(disposed.get()) {
         throw new IllegalStateException("Unable to execute '" + expression + "' as database connection was explicitly closed");
      }
      Statement statement = (Statement)processor.process(expression);
      
      if(statement == null) {
         throw new IllegalStateException("Unable to create a statement for '" + expression + "'");
      }
      return statement;
   }  

   @Override
   public void executeStatement(String expression) throws Exception {
      if(disposed.get()) {
         throw new IllegalStateException("Unable to execute '" + expression + "' as database connection was explicitly closed");
      }
      Statement statement = (Statement)processor.process(expression);
      
      if(statement == null) {
         throw new IllegalStateException("Unable to create a statement for '" + expression + "'");
      }
      statement.execute();
   }

   @Override
   public void closeConnection() throws Exception {
      if(disposed.get()) {
         throw new IllegalStateException("Database connection has alreadt been closed");
      }
      disposed.set(true);
   }
}
