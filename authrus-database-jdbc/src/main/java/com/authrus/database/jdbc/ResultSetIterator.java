package com.authrus.database.jdbc;

import static com.authrus.database.data.DataType.*;
import static java.lang.Integer.MAX_VALUE;
import static java.util.Collections.EMPTY_MAP;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.authrus.database.Counter;
import com.authrus.database.Record;
import com.authrus.database.RecordSchema;
import com.authrus.database.ResultIterator;
import com.authrus.database.data.DataType;

public class ResultSetIterator implements ResultIterator<Record> {   

   private final Statement statement;
   private final RecordSchema schema;
   private final ResultSet result;
   private final String expression;
   private final Counter counter;
   private final Counter mark;

   public ResultSetIterator(Statement statement, ResultSet result, RecordSchema schema, String expression) {
      this.counter = new Counter(1);
      this.mark = new Counter();
      this.expression = expression;
      this.statement = statement;
      this.result = result;
      this.schema = schema;
   }

   @Override
   public synchronized Record next() throws Exception {
      if(result == null) {
         throw new IllegalStateException("No results for statement '" + expression + "'");
      }
      int position = counter.get();
      int current = mark.get();
      
      if(position > current) {
         current = mark.next(); // advance count
      }
      Set<String> names = schema.getColumns();
      
      if(!names.isEmpty()) {
         Map<String, Object> record = new LinkedHashMap<String, Object>();
         
         for(String name : names) {
            Object value = result.getObject(name);
            
            if(value != null) {
               Class type = value.getClass();
               DataType data = DataType.resolveType(type);
               
               if(data == TEXT || data == SYMBOL) {
                  value = result.getString(name); // clob and blob
               }               
               record.put(name, value);
            }
         }
         return new ResultSetRecord(record, schema);
      }
      return new ResultSetRecord(EMPTY_MAP, schema);     
   }
   
   @Override
   public synchronized Record fetchFirst() throws Exception{
      List<Record> records = fetchNext(1);
      
      if(!records.isEmpty()) {
         return records.get(0);
      }
      return null;
   }
   
   @Override
   public synchronized Record fetchLast() throws Exception{
      List<Record> records = fetchAll();
      int length = records.size();
      
      if(length > 0) {         
         return records.get(length - 1);
      }
      return null;
   }   
   
   @Override
   public synchronized List<Record> fetchAll() throws Exception{
      return fetchNext(MAX_VALUE);
   }
   
   @Override
   public synchronized List<Record> fetchNext(int count) throws Exception{
      List<Record> list = new LinkedList<Record>();
      
      for(int i = 0; i < count; i++) {
         if(!hasMore()) {
            return list;
         }
         Record value = next();
         
         if(value != null) {
            list.add(value);
         } 
      }
      return list;
   }  

   @Override
   public synchronized boolean hasMore() throws Exception {  
      try {
         if(result != null) {
            int position = counter.get();
            int current = mark.get();
            
            if(position > current) {
               return true;
            }
            if(result.next()) {
               counter.next(); // advance seek
               return true;
            }
         }
      } catch(Exception e) {
         throw new IllegalStateException("Problem stepping through '" + expression + "'", e);
      }
      return false;
   }
   
   @Override
   public synchronized boolean isEmpty() throws Exception {
      return result == null;
   }
   
   @Override
   public synchronized void close() throws Exception {
      if(statement != null) {
         statement.close();
      }
      if(result != null) {
         result.close();
      }
   }
}
