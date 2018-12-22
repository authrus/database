package com.authrus.database.engine.predicate;

import com.authrus.database.engine.Cell;
import com.authrus.database.engine.Row;

public class NotEqualPredicate extends Predicate {
   
   private final Comparable right;
   private final String name;
   private final int index;
   
   public NotEqualPredicate(Comparable right, String name, int index) {
      this.index = index;
      this.right = right;
      this.name = name;
   }
   
   @Override
   public boolean accept(Row tuple) {
      Cell cell = tuple.getCell(index);
      Comparable left = cell.getValue();         
    
      if(left == null && right == null) {
         return false;
      }
      if(left == null) {
         return true;
      }
      return !left.equals(right);
   }
   
   @Override
   public String toString() {
      return String.format("%s != %s", name, right);
   }   
}