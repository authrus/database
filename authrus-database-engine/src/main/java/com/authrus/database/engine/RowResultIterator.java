package com.authrus.database.engine;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.authrus.database.Counter;
import com.authrus.database.Record;
import com.authrus.database.ResultIterator;
import com.authrus.database.Schema;

public class RowResultIterator implements ResultIterator<Record> {   

   private final List<Row> tuples;
   private final Set<String> names;
   private final Schema schema;
   private final String expression;
   private final Counter counter;
   private final Counter mark;

   public RowResultIterator(Schema schema, List<Row> tuples, Set<String> names, String expression) {
      this.counter = new Counter(1);
      this.mark = new Counter();
      this.expression = expression;
      this.schema = schema;
      this.tuples = tuples;
      this.names = names;
   }

   @Override
   public Record next() throws Exception {
      if(tuples == null) {
         return null;
      }
      int size = tuples.size();
      
      if(size > 0) {
         int position = counter.get();
         int current = mark.get();
         
         if(position > current) {
            current = mark.next(); // advance count
         }
         Row record = tuples.get(0);
         
         if(record == null) {
            throw new IllegalStateException("No result found at cursor position " + current + " for '" + expression + "'");
         }
         return new RowRecord(schema, record, names);
      }
      return null;
   }
   
   @Override
   public Record fetchFirst() throws Exception{
      List<Record> records = fetchNext(1);
      
      if(!records.isEmpty()) {
         return records.get(0);
      }
      return null;
   }
   
   @Override
   public Record fetchLast() throws Exception{
      List<Record> records = fetchAll();
      int length = records.size();
      
      if(length > 0) {         
         return records.get(length - 1);
      }
      return null;
   }
   
   @Override
   public List<Record> fetchAll() throws Exception{
      return fetchNext(Integer.MAX_VALUE);
   }
   
   @Override
   public List<Record> fetchNext(int count) throws Exception{
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
   public boolean hasMore() throws Exception {  
      try {
         if(tuples != null) {
            int size = tuples.size();
            
            if(size == 0) {
               return false;
            }
            int position = counter.get();
            int current = mark.get();
            
            if(position > current) {
               return true;
            }
            if(size > 1) {
               tuples.remove(0); // advance seek
               return true;
            }
         }
      } catch(Exception e) {
         throw new IllegalStateException("Problem stepping through '" + expression + "'", e);
      }
      return false;
   }
   
   @Override
   public boolean isEmpty() throws Exception {
      if(tuples != null) {
         int size = tuples.size();
         
         if(size == 0) {
            return true;
         }
         return false;
      }
      return true;
   }
   
   @Override
   public void close() throws Exception {
      if(tuples != null) {
         tuples.clear();
      }
   }
}
