package com.authrus.database.jdbc;

import static com.authrus.database.sql.ParameterType.NAME;
import static com.authrus.database.sql.ParameterType.VALUE;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.authrus.database.Record;
import com.authrus.database.RecordIterator;
import com.authrus.database.RecordSchema;
import com.authrus.database.ResultCache;
import com.authrus.database.ResultIterator;
import com.authrus.database.sql.Condition;
import com.authrus.database.sql.Parameter;
import com.authrus.database.sql.ParameterType;
import com.authrus.database.sql.Query;
import com.authrus.database.sql.WhereClause;
import com.authrus.database.sql.compile.QueryCompiler;

public class SelectStatementBuilder extends StatementBuilder {
   
   public SelectStatementBuilder(Connection connection, StatementTracer tracer, ResultCache cache, QueryCompiler compiler, Query query) {
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
      int count = conditions.size();
      
      if(count > 0) {
         Object[] list = new Object[count];
         
         for(int i = 0; i < count; i++) {
            Condition condition = conditions.get(i);
            Parameter parameter = condition.getParameter();
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
      List<Record> records = cache.check(table, expression);
      
      if(records != null) {
         return new RecordIterator(records, expression);
      }      
      try {
         java.sql.Statement statement = connection.createStatement();
         ResultSet result = statement.executeQuery(expression);
         List<Record> list = extract(statement, result, expression);
         
         if(list != null) {
            cache.update(table, expression, list);
         }
         if(!statement.isClosed()) {
            statement.close();
         }
         if(!result.isClosed()) {
            result.close();
         }
         return new RecordIterator(list, expression);
      } finally {
         long finish = System.currentTimeMillis();
         long duration = finish - start;
         
         tracer.traceStatement(expression, duration);
      }
   }

   private List<Record> extract(java.sql.Statement statement, ResultSet result, String expression) throws Exception {
      RecordSchema schema = schema(result);
      
      if(result.next()) {
         List<Record> records = new ArrayList<Record>();
         ResultIterator<Record> iterator = new ResultSetIterator(statement, result, schema, expression);
         
         while(iterator.hasMore()) {
            Record record = iterator.next();
            
            if(record != null) {
               records.add(record);
            }
         }         
         return records;
      }
      return Collections.emptyList();
   }
   
   private RecordSchema schema(ResultSet result) throws Exception {
      ResultSetMetaData data = result.getMetaData();
      List<String> columns = query.getColumns();
      String source = query.getSource();
      int count = data.getColumnCount();
      int require = columns.size();
      
      if(require > 0 && count != require) {
         throw new IllegalStateException("Cursor contains '" + count + "' columns but expression '" + source + "' defines " + require);
      }
      RecordSchema schema = new RecordSchema();

      for(int i = 0; i < count; i++) {
         String title = data.getColumnName(i + 1);               
         
         if(title == null) {
            throw new IllegalStateException("Column at index " + i + " was null for '" + source + "'");
         }
         if(require > i) {
            String column = columns.get(i);
            
            if(!title.equalsIgnoreCase(column)) {
              if(!column.equalsIgnoreCase("count(*)")) {
                 throw new IllegalStateException("Column at index " + i + " should be '" + column + "' but is '" + title + "' for '" + source + "'");
              } else {
                 schema.addColumn(title);
              }
            } else {
               schema.addColumn(column);
            }
         } else {
            schema.addColumn(title);
         }
      }
      return schema;
   }
}
