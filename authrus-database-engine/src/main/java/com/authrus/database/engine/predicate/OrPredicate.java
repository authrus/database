package com.authrus.database.engine.predicate;

import com.authrus.database.engine.Row;

public class OrPredicate extends Predicate {
   
   private final Predicate left;
   private final Predicate right;
   
   public OrPredicate(Predicate left, Predicate right) {
      this.left = left;
      this.right = right;
   }
   
   @Override
   public boolean accept(Row tuple) {
      return left.accept(tuple) || right.accept(tuple);
   }
   
   @Override
   public String toString() {
      return String.format("(%s) or (%s)", left, right);
   }      
}
