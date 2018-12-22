package com.authrus.database.engine.filter;

public class QueryState {

   private final Comparable[] parameters;
   
   public QueryState() {
      this(null);
   }
   
   public QueryState(Comparable[] parameters) {
      this.parameters = parameters;
   }
   
   public Comparable getValue(int index) {
      if(parameters == null) {
         return null;
      }
      if(index < 0 || index >= parameters.length) {
         return null;
      }
      return parameters[index];
   }
}
