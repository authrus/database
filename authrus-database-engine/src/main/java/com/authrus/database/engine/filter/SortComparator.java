package com.authrus.database.engine.filter;

import java.util.Comparator;

import com.authrus.database.Column;
import com.authrus.database.engine.Cell;
import com.authrus.database.engine.Row;

public class SortComparator implements Comparator<Row> {
   
   private final String name;
   private final int index;
   private final boolean ascending;
   
   public SortComparator(Column column, boolean ascending) {
      this.index = column.getIndex();
      this.name = column.getName();
      this.ascending = ascending;    
   }

   @Override
   public int compare(Row left, Row right) {
      Cell leftCell = left.getCell(index);
      Cell rightCell = right.getCell(index);
      
      if(leftCell == null || rightCell == null) {
         throw new IllegalStateException("Unable to sort by '" + name + "' as it does not exist");
      }
      Comparable leftValue = leftCell.getValue();
      Comparable rightValue = rightCell.getValue();
      
      if(ascending) {
         return leftValue.compareTo(rightValue);
      }
      return rightValue.compareTo(leftValue);
   }   
}
