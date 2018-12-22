package com.authrus.database.jdbc;

import static com.authrus.database.sql.ParameterType.NAME;
import static com.authrus.database.sql.ParameterType.VALUE;
import static java.util.Collections.EMPTY_LIST;

import java.sql.Connection;
import java.util.List;

import com.authrus.database.Record;
import com.authrus.database.RecordIterator;
import com.authrus.database.ResultCache;
import com.authrus.database.ResultIterator;
import com.authrus.database.sql.Parameter;
import com.authrus.database.sql.ParameterType;
import com.authrus.database.sql.Query;
import com.authrus.database.sql.compile.QueryCompiler;

public class InsertStatementBuilder extends StatementBuilder {
   
   public InsertStatementBuilder(Connection connection, StatementTracer tracer, ResultCache cache, QueryCompiler compiler, Query query) {
      super(connection, tracer, cache, compiler, query);
   }
   
   @Override
   public synchronized ResultIterator<Record> execute() throws Exception {
      boolean disposed = closed.get();
      
      if(disposed) {
         throw new IllegalStateException("This statement has been closed");
      }
      List<Parameter> parameters = query.getParameters();
      int parameterCount = parameters.size();
      
      if(parameterCount > 0) { 
         Object[] list = new Object[parameterCount];
         
         for(int i = 0; i < parameterCount; i++) {
            Parameter parameter = parameters.get(i);
            String parameterName = parameter.getName();
            ParameterType parameterType = parameter.getType();
            
            if(parameterType == VALUE) {
               list[i] = parameter.getValue();
            } else if(parameterType == NAME) {
               list[i] = attributes.get(parameterName);
            } else {
               throw new IllegalStateException("Parameter '" + parameter + "' is not a named or literal parameter");            
            }
         }
         return execute(list);
      }
      return execute(EMPTY);
   }  
   
   protected synchronized ResultIterator<Record> execute(Object[] values) throws Exception {
      long start = System.currentTimeMillis();
      boolean disposed = closed.get();
      
      if(disposed) {
         throw new IllegalStateException("This statement has been closed");
      }    
      String source = query.getSource();
      List<Parameter> parameters = query.getParameters();
      List<String> columns = query.getColumns();
      int parameterCount = parameters.size();
      int columnCount = columns.size();
      
      if(values.length != parameterCount) {
         throw new IllegalArgumentException("Expected " + parameterCount + " parameters but got " + values.length + " for " + source);
      } 
      if(columnCount != parameterCount) {
         throw new IllegalArgumentException("There are " + columnCount + " columns but " + parameterCount + " parameters for " + source);
      }
      String table = query.getTable();
      String expression = compiler.compile(query, values);
      java.sql.Statement statement = connection.createStatement();
      
      try {         
         try {
            statement.execute(expression);
            cache.clear(table);
         } finally {
            statement.close();
         }   
         return new RecordIterator(EMPTY_LIST, expression);
      } finally {
         long finish = System.currentTimeMillis();
         long duration = finish - start;
         
         tracer.traceStatement(expression, duration);
      }
   }
}
