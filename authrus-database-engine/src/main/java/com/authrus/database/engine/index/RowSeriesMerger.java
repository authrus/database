package com.authrus.database.engine.index;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.authrus.database.Column;
import com.authrus.database.engine.Row;
import com.authrus.database.engine.predicate.Predicate;
import com.authrus.database.engine.predicate.PredicateBuilder;

public class RowSeriesMerger  {   
   
   private final PredicateBuilder builder;
   
   public RowSeriesMerger() {
      this.builder = new PredicateBuilder();
   }
   
   public RowSeries merge(RowSeries left, RowSeries right) {
      RowCursor leftCursor = left.createCursor();
      RowCursor rightCursor = right.createCursor();
      
      return new MergeSeries(leftCursor, rightCursor);
   }

   private class MergeSeries implements RowSeries {
      
      private final MergeCursor cursor;
      
      public MergeSeries(RowCursor left, RowCursor right) {
         this.cursor = new MergeCursor(left, right);
      }
      
      @Override
      public RowCursor createCursor() {
         return cursor;
      }   

      @Override
      public RowSeries greaterThan(Column column, Comparable value) {
         return where(column, value, ">");
      }

      @Override
      public RowSeries lessThan(Column column, Comparable value) {
         return where(column, value, "<");
      }

      @Override
      public RowSeries greaterThanOrEqual(Column column, Comparable value) {
         return where(column, value, ">=");
      }

      @Override
      public RowSeries lessThanOrEqual(Column column, Comparable value) {
         return where(column, value, "<=");
      }

      @Override
      public RowSeries equalTo(Column column, Comparable value) {
         return where(column, value, "==");
      }

      @Override
      public RowSeries notEqualTo(Column column, Comparable value) {
         return where(column, value, "!=");
      }
      
      @Override
      public RowSeries like(Column column, Comparable value) {
         return where(column, value, "=~");
      }      
      
      protected RowSeries where(Column column, Comparable value, String operator) {
         Predicate comparison = builder.compare(column, value, operator);        
         
         return new PredicateSeries(cursor, comparison);
      }  
   }
   
   private class MergeCursor implements RowCursor {
      
      private final RowCounter counter;
      private final RowCursor left;
      private final RowCursor right;
      
      public MergeCursor(RowCursor left, RowCursor right) {
         this.counter = new RowCounter(this);
         this.left = left;
         this.right = right;
      }

      @Override
      public Iterator<Row> iterator() {
         Iterator<Row> leftIterator = left.iterator();
         Iterator<Row> rightIterator = right.iterator();
         
         return new MergeIterator(leftIterator, rightIterator);
      }

      @Override
      public int count() {
         return counter.count();
      }      
   }
   
   private class MergeIterator implements Iterator<Row> {
      
      private Iterator<Row> left;
      private Iterator<Row> right;
      private Set<String> done;
      private Row next;
      
      public MergeIterator(Iterator<Row> left, Iterator<Row> right) {
         this.done = new HashSet<String>();
         this.left = left;
         this.right = right;
      }

      @Override
      public boolean hasNext() {
         if(next == null) {
            while(left.hasNext()) {
               Row tuple = left.next();
               String key = tuple.getKey();
               
               if(done.add(key)) {
                  next = tuple;
                  return true;
               }               
            }
            while(right.hasNext()) {
               Row tuple = right.next();
               String key = tuple.getKey();
               
               if(done.add(key)) {
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
