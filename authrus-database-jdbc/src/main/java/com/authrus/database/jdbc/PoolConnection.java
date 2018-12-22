package com.authrus.database.jdbc;

import java.sql.Connection;
import java.util.concurrent.atomic.AtomicBoolean;

import com.authrus.database.DatabaseConnection;
import com.authrus.database.Statement;
import com.authrus.database.sql.build.QueryProcessor;

public class PoolConnection implements DatabaseConnection {
   
   private final QueryProcessor processor;
   private final AtomicBoolean closed;
   private final Connection connection;
   
   public PoolConnection(Connection connection, QueryProcessor processor) {
      this.closed = new AtomicBoolean();
      this.connection = connection;
      this.processor = processor;
   } 

   @Override
   public synchronized void executeStatement(String expression) throws Exception {     
      boolean disposed = closed.get();
      
      if(disposed) {
         throw new IllegalStateException("Unable to execute '" + expression + "' as database connection was explicitly closed");
      }
      Statement statement = (Statement)processor.process(expression);
      
      if(statement == null) {
         throw new IllegalStateException("Unable to create a statement for '" + expression + "'");
      }
      statement.execute();
   }

   @Override
   public synchronized Statement prepareStatement(String expression) throws Exception {
      boolean disposed = closed.get();
      
      if(disposed) {
         throw new IllegalStateException("Unable to execute '" + expression + "' as database connection was explicitly closed");
      }
      return prepareStatement(expression, true);
   }
   
   @Override
   public synchronized Statement prepareStatement(String expression, boolean cache) throws Exception {
      boolean disposed = closed.get();
      
      if(disposed) {
         throw new IllegalStateException("Unable to execute '" + expression + "' as database connection was explicitly closed");
      }
      Statement statement = (Statement)processor.process(expression);
      
      if(statement == null) {
         throw new IllegalStateException("Unable to create a statement for '" + expression + "'");
      }
      return statement;
   }  

   @Override
   public synchronized void closeConnection() throws Exception {
      boolean disposed = closed.get();
      
      if(disposed) {
         throw new IllegalStateException("Database connection has alreadt been closed");
      }
      closed.set(true);
      connection.close();
   }
}
