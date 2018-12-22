package com.authrus.database.engine.filter;

import java.util.Iterator;

import com.authrus.database.engine.Row;

public class LimitIterator implements Iterator<Row>{
   
   private Iterator<Row> iterator;
   private int limit;
   
   public LimitIterator(Iterator<Row> iterator, int limit) {
      this.iterator = iterator;
      this.limit = limit;
   }
   
   @Override
   public boolean hasNext() {
      if(limit > 0) {
         return iterator.hasNext();
      }
      return false;
   }
   
   @Override
   public Row next() {
      if(limit > 0) {
         Row tuple = iterator.next();
         
         if(tuple != null) {
            limit--;
         }
         return tuple;
      }
      return null;
   }
   
   @Override
   public void remove() {
      throw new UnsupportedOperationException("Remove not supported");
   }
}
