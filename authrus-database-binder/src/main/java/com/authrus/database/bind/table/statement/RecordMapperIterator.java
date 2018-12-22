package com.authrus.database.bind.table.statement;

import java.util.LinkedList;
import java.util.List;

import com.authrus.database.Record;
import com.authrus.database.ResultIterator;

public class RecordMapperIterator implements ResultIterator {
   
   private final ResultIterator<Record> iterator;
   private final RecordMapper mapper;
   
   public RecordMapperIterator(ResultIterator<Record> iterator, RecordMapper mapper) {
      this.iterator = iterator;
      this.mapper = mapper;
   }

   @Override
   public Object next() throws Exception{
      Record record = iterator.next();
      
      if(record != null) {
         return mapper.toObject(record);
      }
      return null;
   }

   @Override
   public Object fetchFirst() throws Exception{
      List<Object> records = fetchNext(2);
      
      if(!records.isEmpty()) {
         return records.get(0);
      }
      return null;
   }
   
   @Override
   public Object fetchLast() throws Exception{
      List<Object> records = fetchAll();
      int length = records.size();
      
      if(length > 0) {         
         return records.get(length - 1);
      }
      return null;
   }
   
   @Override
   public List<Object> fetchAll() throws Exception{
      return fetchNext(Integer.MAX_VALUE);
   }
   
   @Override
   public List<Object> fetchNext(int count) throws Exception{
      List<Object> list = new LinkedList<Object>();
      
      for(int i = 0; i < count; i++) {
         if(!hasMore()) {
            return list;
         }
         Object value = next();
         
         if(value != null) {
            list.add(value);
         } 
      }
      return list;
   }

   @Override
   public boolean hasMore() throws Exception {
      return iterator.hasMore();
   }

   @Override
   public boolean isEmpty() throws Exception {
      return iterator.isEmpty();
   }

   @Override
   public void close() throws Exception{
      iterator.close();
   }
}
