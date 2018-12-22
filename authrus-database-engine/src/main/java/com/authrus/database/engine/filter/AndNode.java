package com.authrus.database.engine.filter;

import com.authrus.database.engine.index.RowSeries;

public class AndNode implements FilterNode {
   
   private final FilterNode left;
   private final FilterNode right;
   
   public AndNode(FilterNode left, FilterNode right) {      
      this.left = left;
      this.right = right;
   }   

   @Override
   public RowSeries apply(RowSeries series) {
      RowSeries result = left.apply(series);
      
      if(result != null) {
         return right.apply(result);
      }
      return right.apply(series);
   }

   @Override
   public String toString() {
      return String.format("(%s) and (%s)", left, right);
   }
}
