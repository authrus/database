package com.authrus.database.engine.filter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.authrus.database.engine.Row;
import com.authrus.database.engine.index.RowCursor;
import com.authrus.database.engine.index.RowSeries;

public class NoFilter implements Filter {
   
   private final SortComparator comparator;
   private final int limit;

   public NoFilter() {
      this(null);
   }
   
   public NoFilter(SortComparator comparator) {
      this(comparator, 0);
   }
   
   public NoFilter(SortComparator comparator, int limit) {
      this.comparator = comparator;
      this.limit = limit;
   }
   
   @Override
   public Iterator<Row> select(RowSeries series) {
      RowCursor cursor = series.createCursor(); 
      Iterator<Row> iterator = cursor.iterator();
      
      if(comparator != null) {              
         List<Row> tuples = new ArrayList<Row>();
         
         while(iterator.hasNext()) {
            Row tuple = iterator.next();
            
            if(tuple != null) {
               tuples.add(tuple);
            }
         }   
         int size = tuples.size();
         
         if(size > 1) {
            Collections.sort(tuples, comparator);
         }
         iterator = tuples.iterator();
      }
      if(limit > 0) {
         return new LimitIterator(iterator, limit);
      }
      return iterator;
   }

   @Override
   public int count(RowSeries series) {
      RowCursor cursor = series.createCursor();
      int count = cursor.count();
      
      if(limit > 0) {
         return Math.min(limit, count);
      }
      return count;
   }   
   
   @Override
   public String toString() {
      return "*";
   }
}
