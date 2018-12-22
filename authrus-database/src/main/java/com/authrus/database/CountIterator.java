package com.authrus.database;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class CountIterator implements ResultIterator<Record> {
   
   private final AtomicBoolean disposed;
   private final Count count;
   
   public CountIterator(String column, int count) {
      this.disposed = new AtomicBoolean(false);
      this.count = new Count(column, count);
   }

   @Override
   public Record next() throws Exception {
      disposed.set(true);
      return count;
   }
   
   @Override
   public Record fetchFirst() throws Exception{
      return next();
   }
   
   @Override
   public Record fetchLast() throws Exception{
      return next();
   }

   @Override
   public List<Record> fetchAll() throws Exception {
      return Arrays.<Record>asList(count);
   }

   @Override
   public List<Record> fetchNext(int count) throws Exception {
      return fetchAll();
   }

   @Override
   public boolean hasMore() throws Exception {
      return !disposed.get();
   }

   @Override
   public boolean isEmpty() throws Exception {
      return false;
   }

   @Override
   public void close() throws Exception {
      disposed.set(true);
   }
   
   private static class Count implements Record {
      
      private final Integer count;
      private final String column;
      
      public Count(String column, Integer count) {
         this.column = column;
         this.count = count;
      }

      @Override
      public Set<String> getColumns() throws Exception {        
         return Collections.singleton(column);
      }


      @Override
      public Integer getInteger(String name) throws Exception {
         if(!name.equalsIgnoreCase(column)) {
            throw new IllegalStateException("Column '" + name + "' does not exist for schema '" + column + "'");
         }
         return count;         
      }

      @Override
      public String getString(String name) throws Exception {
         if(!name.equalsIgnoreCase(column)) {
            throw new IllegalStateException("Column '" + name + "' does not exist for schema '" + column + "'");
         }
         return String.valueOf(count);  
      }

      @Override
      public Double getDouble(String name) throws Exception {
         if(!name.equalsIgnoreCase(column)) {
            throw new IllegalStateException("Column '" + name + "' does not exist for schema '" + column + "'");
         }
         return count.doubleValue();  
      }    

      @Override
      public Long getLong(String name) throws Exception {
         if(!name.equalsIgnoreCase(column)) {
            throw new IllegalStateException("Column '" + name + "' does not exist for schema '" + column + "'");
         }
         return count.longValue();
      }      

      @Override
      public Float getFloat(String name) throws Exception {
         if(!name.equalsIgnoreCase(column)) {
            throw new IllegalStateException("Column '" + name + "' does not exist for schema '" + column + "'");
         }
         return count.floatValue();
      }

      @Override
      public Byte getByte(String name) throws Exception {
         if(!name.equalsIgnoreCase(column)) {
            throw new IllegalStateException("Column '" + name + "' does not exist for schema '" + column + "'");
         }
         return count.byteValue();
      }

      @Override
      public Short getShort(String name) throws Exception {
         if(!name.equalsIgnoreCase(column)) {
            throw new IllegalStateException("Column '" + name + "' does not exist for schema '" + column + "'");
         }
         return count.shortValue();
      }      

      @Override
      public Character getCharacter(String name) throws Exception {
         if(!name.equalsIgnoreCase(column)) {
            throw new IllegalStateException("Column '" + name + "' does not exist for schema '" + column + "'");
         }
         throw new IllegalStateException("Value of '" + column + "' is not a character");
      }      
      
      @Override
      public Boolean getBoolean(String name) throws Exception {
         if(!name.equalsIgnoreCase(column)) {
            throw new IllegalStateException("Column '" + name + "' does not exist for schema '" + column + "'");
         }
         throw new IllegalStateException("Value of '" + column + "' is not a boolean");
      }            
      
      @Override
      public Date getDate(String name) throws Exception {
         if(!name.equalsIgnoreCase(column)) {
            throw new IllegalStateException("Column '" + name + "' does not exist for schema '" + column + "'");
         }
         throw new IllegalStateException("Value of '" + column + "' is not a date");
      }       
   }

}
