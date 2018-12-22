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
import com.authrus.database.sql.Condition;
import com.authrus.database.sql.Parameter;
import com.authrus.database.sql.ParameterType;
import com.authrus.database.sql.Query;
import com.authrus.database.sql.WhereClause;
import com.authrus.database.sql.compile.QueryCompiler;

public class UpdateStatementBuilder extends StatementBuilder {
   
   public UpdateStatementBuilder(Connection connection, StatementTracer tracer, ResultCache cache, QueryCompiler compiler, Query query) {
      super(connection, tracer, cache, compiler, query);
   }
   
   @Override
   public synchronized ResultIterator<Record> execute() throws Exception {
      boolean disposed = closed.get();
      
      if(disposed) {
         throw new IllegalStateException("This statement has been closed");
      }
      WhereClause clause = query.getWhereClause();
      List<Condition> conditions = clause.getConditions();
      List<Parameter> parameters = query.getParameters();
      int conditionCount = conditions.size();
      int parameterCount = parameters.size();
      int totalCount = parameterCount + conditionCount;
      
      if(totalCount > 0) {
         Object[] list = new Object[totalCount];
         
         for(int i = 0; i < parameterCount; i++) {
            Parameter parameter = parameters.get(i);
            String name = parameter.getName();
            ParameterType type = parameter.getType();
            
            if(type == VALUE) {
               list[i] = parameter.getValue();
            } else if(type == NAME) {
               list[i] = attributes.get(name);  
            } else {
               throw new IllegalStateException("Parameter '" + parameter + "' is not a named or literal parameter");            
            }        
         }
         for(int i = 0; i < conditionCount; i++) {
            Condition condition = conditions.get(i);
            Parameter parameter = condition.getParameter();
            String name = parameter.getName();
            ParameterType type = parameter.getType();
            
            if(type == VALUE) {
               list[i + parameterCount] = parameter.getValue();
            } else if(type == NAME) {
               list[i + parameterCount] = attributes.get(name);  
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
