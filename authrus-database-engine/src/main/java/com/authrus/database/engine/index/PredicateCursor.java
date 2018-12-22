package com.authrus.database.engine.index;

import java.util.Iterator;

import com.authrus.database.engine.Row;
import com.authrus.database.engine.predicate.Predicate;

public class PredicateCursor implements RowCursor {
   
   private Predicate predicate;
   private RowCounter counter;
   private RowCursor cursor;
   
   public PredicateCursor(RowCursor cursor, Predicate predicate) {
      this.counter = new RowCounter(this);
      this.predicate = predicate;
      this.cursor = cursor;
   }   

   @Override
   public Iterator<Row> iterator() {
      return new PredicateIterator(cursor, predicate);
   }
   
   @Override
   public int count() {
      return counter.count();
   }
   
   private class PredicateIterator implements Iterator<Row> {      
      
      private Iterator<Row> iterator;
      private Predicate predicate;
      private Row next;
      
      public PredicateIterator(RowCursor cursor, Predicate predicate) {
         this.iterator = cursor.iterator();
         this.predicate = predicate;
      }        

      @Override
      public boolean hasNext() {
         if(next == null) {
            while(iterator.hasNext()) {
               Row tuple = iterator.next();
               
               if(predicate.accept(tuple)) {
                  next = tuple;
                  return true;
               }
            }
            return false;
         }     
         return true;
      }
   
      @Override
      public Row next() {
         Row result = next;
         
         if(result == null) {
            if(hasNext()) {
               result = next;
            }
         }
         if(result != null) {
            next = null;
         }
         return result;   
      }
   
      @Override
      public void remove() {
         throw new UnsupportedOperationException("Remove is not supported");  
      }
   }
}
