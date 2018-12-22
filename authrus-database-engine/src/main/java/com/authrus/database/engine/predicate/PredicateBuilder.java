package com.authrus.database.engine.predicate;

import com.authrus.database.Column;

public class PredicateBuilder {

   public Predicate combine(Predicate left, Predicate right, String token) {
      if(token.equalsIgnoreCase("and")) {
         return new AndPredicate(left, right);
      }
      if(token.equalsIgnoreCase("or")) {
         return new OrPredicate(left, right);
      }
      throw new IllegalArgumentException("Unknown combination operator '" + token + "'");
   }   
   
   public Predicate compare(Column column, Comparable value, String token) {
      String name = column.getName();
      int index = column.getIndex();
      
      if(token.equals("==")) {
         return new EqualPredicate(value, name, index);
      }
      if(token.equals("!=")) {
         return new NotEqualPredicate(value, name, index);
      } 
      if(token.equals(">")) {
         return new GreaterThanPredicate(value, name, index);
      } 
      if(token.equals("<")) {
         return new LessThanPredicate(value, name, index);
      }
      if(token.equals(">=")) {
         return new GreaterThanOrEqualPredicate(value, name, index);
      } 
      if(token.equals("<=")) {
         return new LessThanOrEqualPredicate(value, name, index);
      }
      if(token.equals("=~")) {
         return new LikePredicate(value, name, index);
      }       
      if(token.equals("like")) {
         return new LikePredicate(value, name, index);
      }         
      throw new IllegalArgumentException("Unknown comparison operator '" + token + "'");
   }
}
