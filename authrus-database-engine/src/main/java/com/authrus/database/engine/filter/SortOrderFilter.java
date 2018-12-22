package com.authrus.database.engine.filter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.authrus.database.engine.Row;
import com.authrus.database.engine.index.RowSeries;

public class SortOrderFilter implements Filter {
   
   private final SortComparator comparator;   
   private final Filter filter;
   private final int limit;

   public SortOrderFilter(FilterNode root, SortComparator comparator){
      this(root, comparator, 0);
   }
   
   public SortOrderFilter(FilterNode root, SortComparator comparator, int limit){
      this.filter = new RandomOrderFilter(root, 0);      
      this.comparator = comparator;
      this.limit = limit;
   }
   
   @Override
   public Iterator<Row> select(RowSeries series) {      
      Iterator<Row> iterator = filter.select(series);
      
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
      int count = filter.count(series);
      
      if(limit > 0) {
         return Math.min(limit, count);
      }
      return count;
   }    
   
   @Override
   public String toString() {
      return String.valueOf(filter);
   } 
}
