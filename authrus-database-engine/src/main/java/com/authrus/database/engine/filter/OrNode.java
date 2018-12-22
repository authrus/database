package com.authrus.database.engine.filter;

import com.authrus.database.engine.index.RowSeries;
import com.authrus.database.engine.index.RowSeriesMerger;

public class OrNode implements FilterNode {
   
   private final RowSeriesMerger merger;
   private final FilterNode left;
   private final FilterNode right;
   
   public OrNode(FilterNode left, FilterNode right) {
      this.merger = new RowSeriesMerger();
      this.left = left;
      this.right = right;
   }

   @Override
   public RowSeries apply(RowSeries series) {
      RowSeries leftSeries = left.apply(series);
      RowSeries rightSeries = right.apply(series);
      
      return merger.merge(leftSeries, rightSeries);
   }
   
   @Override
   public String toString() {
      return String.format("(%s) or (%s)", left, right);
   }
}
