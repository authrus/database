package com.authrus.database.bind.table.statement;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.authrus.database.Database;
import com.authrus.database.DatabaseConnection;
import com.authrus.database.Record;
import com.authrus.database.ResultIterator;
import com.authrus.database.Schema;
import com.authrus.database.Statement;
import com.authrus.database.bind.table.TableContext;
import com.authrus.database.data.DataConverter;
import com.authrus.database.data.DataType;

public class SelectCountStatement<T> {

   private final AtomicReference<String> whereClause;
   private final AtomicReference<String> orderClause;
   private final Map<String, Comparable> parameters;
   private final TableContext<T> context;
   private final AtomicInteger limit;
   private final Database database;

   public SelectCountStatement(Database database, TableContext<T> context) {
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
   
   public SelectCountStatement<T> set(String name, Comparable value) {
      parameters.put(name, value);
      return this;
   }
   
   public SelectCountStatement<T> where(String clause) {
      whereClause.set(clause);
      return this;
   }  
   
   public SelectCountStatement<T> orderBy(String clause) {
      orderClause.set(clause);
      return this;
   }
   
   public SelectCountStatement<T> limit(int count) {
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
      
      builder.append("select count(*) from ");
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

   public int execute() throws Exception {
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
         
         if(!iterator.isEmpty()) {
            Record record = iterator.next();
           
            if(record != null) {
               return record.getInteger("count(*)");
            }
         }
      } finally {
         connection.closeConnection();
      }
      return 0;    
   }
}
