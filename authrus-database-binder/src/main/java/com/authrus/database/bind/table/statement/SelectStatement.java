package com.authrus.database.bind.table.statement;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.authrus.database.Column;
import com.authrus.database.Database;
import com.authrus.database.DatabaseConnection;
import com.authrus.database.Record;
import com.authrus.database.ResultIterator;
import com.authrus.database.Schema;
import com.authrus.database.Statement;
import com.authrus.database.bind.table.TableContext;
import com.authrus.database.data.DataConverter;
import com.authrus.database.data.DataType;

public class SelectStatement<T> {

   private final AtomicReference<String> whereClause;
   private final AtomicReference<String> orderClause;
   private final Map<String, Comparable> parameters;
   private final TableContext<T> context;
   private final AtomicInteger limit;
   private final Database database;
   
   public SelectStatement(Database database, TableContext<T> context) {
      this.parameters = new LinkedHashMap<String, Comparable>();
      this.whereClause = new AtomicReference<String>();
      this.orderClause = new AtomicReference<String>();
      this.limit = new AtomicInteger();
      this.database = database;
      this.context = context;
   }   
   
   public Object get(String name) {
      return parameters.get(name);
   }   
   
   public SelectStatement<T> set(String name, Comparable value) {
      parameters.put(name, value);
      return this;
   }
   
   public SelectStatement<T> where(String clause) {
      whereClause.set(clause);
      return this;
   }  
   
   public SelectStatement<T> orderBy(String clause) {
      orderClause.set(clause);
      return this;
   }
   
   public SelectStatement<T> limit(int count) {
      limit.set(count);
      return this;
   }
   
   public String compile() throws Exception {
      String table = context.getName();
      Schema schema = context.getSchema();
      String whereExpression = whereClause.get();
      String orderExpression = orderClause.get();
      int columnCount = schema.getCount();
      
      if(columnCount <= 0) {
         throw new IllegalStateException("Table '" + table + "' has " + columnCount + " selectable columns");
      }
      StringBuilder builder = new StringBuilder();
      
      builder.append("select ");
      
      for(int i = 0; i < columnCount; i++) {
         Column column = schema.getColumn(i);
         String columnTitle = column.getTitle();
         
         if(i > 0) {
            builder.append(", ");
         }
         builder.append(columnTitle);
      }
      builder.append(" from ");
      builder.append(table);
      
      if(whereExpression != null) {
         builder.append(" where ");
         builder.append(whereExpression);
      }
      if(orderExpression != null) {
         builder.append(" order by ");
         builder.append(orderExpression);
      }
      int count = limit.get();
      
      if(count > 0) {
         builder.append(" limit ");
         builder.append(count);
      }
      return builder.toString();
   }    

   public ResultIterator<T> execute() throws Exception {
      String table = context.getName();
      Schema schema = context.getSchema();
      int columnCount = schema.getCount();
      
      if(columnCount <= 0) {
         throw new IllegalStateException("Table '" + table + "' has " + columnCount + " selectable columns");
      }
      String expression = compile();
      DatabaseConnection connection = database.getConnection();
      
      try {
         Statement statement = connection.prepareStatement(expression);
         DataConverter converter = context.getConverter();  
         Set<String> names = parameters.keySet();
         
         for(String name : names) {
            Comparable value = parameters.get(name);
            
            if(value != null) {
               Class type = value.getClass();
               DataType match = DataType.resolveType(type);
               Comparable result = converter.convert(match, value);
                     
               match.setData(statement, name, result);
            }
         } 
         ResultIterator<Record> iterator = statement.execute();   
         RecordMapper<T> mapper = context.getMapper();
         
         return new RecordMapperIterator(iterator, mapper);
      } finally {
         connection.closeConnection();
      }
   }
}
