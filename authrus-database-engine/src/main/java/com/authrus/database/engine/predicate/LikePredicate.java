package com.authrus.database.engine.predicate;

import com.authrus.database.engine.Cell;
import com.authrus.database.engine.Row;

public class LikePredicate extends Predicate {

   private final LikeEvaluator evaluator;
   private final Comparable right;
   private final String name;
   private final int index;
   
   public LikePredicate(Comparable right, String name, int index) {
      this.evaluator = new LikeEvaluator(right);
      this.right = right;
      this.index = index;
      this.name = name;
   }
   
   @Override
   public boolean accept(Row tuple) {
      Cell cell = tuple.getCell(index);
      Comparable left = cell.getValue();         
    
      if(left != null) {
         String source = String.valueOf(left);
      
         if(right != null) {
            return evaluator.like(source);
         }
      }
      return false;
   }
   
   @Override
   public String toString() {
      return String.format("%s like %s", name, right);
   } 
}
