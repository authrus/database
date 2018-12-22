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

public class DeleteStatementBuilder extends StatementBuilder {
   
   public DeleteStatementBuilder(Connection connection, StatementTracer tracer, ResultCache cache, QueryCompiler compiler, Query query) {
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
      int conditionCount = conditions.size();
      int attributeCount = attributes.size();
      
      if(attributeCount != conditionCount) {
         throw new IllegalArgumentException("Expected " + conditionCount + " parameters but got " + attributeCount + " with " + attributes);
      } 
      if(conditionCount > 0) {
         Object[] list = new Object[conditionCount];
         
         for(int i = 0; i < conditionCount; i++) {
            Condition condition = conditions.get(i);
            Parameter parameter = condition.getParameter();
            ParameterType parameterType = parameter.getType();
            String name = parameter.getName();
            
            if(parameterType == VALUE) {
               list[i] = parameter.getValue();
            } else if(parameterType == NAME) {
               list[i] = attributes.get(name);
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
