package com.authrus.database;

import java.util.LinkedList;
import java.util.List;

public class RecordIterator implements ResultIterator<Record> {   

   private final List<Record> records;
   private final String expression;
   private final Counter counter;
   private final Counter mark;

   public RecordIterator(List<Record> records, String expression) {
      this.counter = new Counter(1);
      this.mark = new Counter();
      this.expression = expression;
      this.records = records;
   }

   @Override
   public synchronized Record next() throws Exception {
      if(records == null) {
         return null;
      }
      int size = records.size();
      
      if(size > 0) {
         int position = counter.get();
         int current = mark.get();
         
         if(position > current) {
            current = mark.next(); // advance count
         }
         Record record = records.get(0);
         
         if(record == null) {
            throw new IllegalStateException("No result found at cursor position " + current + " for '" + expression + "'");
         }
         return record;
      }
      return null;
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
      return fetchNext(Integer.MAX_VALUE);
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
         if(records != null) {
            int size = records.size();
            
            if(size == 0) {
               return false;
            }
            int position = counter.get();
            int current = mark.get();
            
            if(position > current) {
               return true;
            }
            if(size > 1) {
               records.remove(0); // advance seek
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
      if(records != null) {
         int size = records.size();
         
         if(size == 0) {
            return true;
         }
         return false;
      }
      return true;
   }
   
   @Override
   public synchronized void close() throws Exception {
      if(records != null) {
         records.clear();
      }
   }
}
