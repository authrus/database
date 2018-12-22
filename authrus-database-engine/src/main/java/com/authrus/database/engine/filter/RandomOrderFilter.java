package com.authrus.database.engine.filter;

import java.util.Iterator;

import com.authrus.database.engine.Row;
import com.authrus.database.engine.index.RowCursor;
import com.authrus.database.engine.index.RowSeries;

public class RandomOrderFilter implements Filter {
   
   private final FilterNode root;
   private final int limit;

   public RandomOrderFilter(FilterNode root) {
      this(root, 0);
   }
   
   public RandomOrderFilter(FilterNode root, int limit) {
      this.limit = limit;
      this.root = root;
   }

   @Override
   public Iterator<Row> select(RowSeries series) {
      RowSeries result = root.apply(series);
      RowCursor cursor = result.createCursor();      
      Iterator<Row> iterator = cursor.iterator();
      
      if(limit > 0) {
         return new LimitIterator(iterator, limit);
      }
      return iterator;
   }   

   @Override
   public int count(RowSeries series) {
      RowSeries result = root.apply(series);
      RowCursor cursor = result.createCursor();  
      int count = cursor.count();
      
      if(limit > 0) {
         return Math.min(limit, count);
      }
      return count;
   }   
   
   @Override
   public String toString() {
      return String.valueOf(root);
   }

}
