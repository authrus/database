package com.authrus.database.sql;

public class Condition {
   
   public final Parameter parameter;
   public final String comparison;   
   public final String expression;

   public Condition(Parameter parameter, String comparison, String expression) {
      this.expression = expression;
      this.parameter = parameter;
      this.comparison = comparison;
   }
   
   public Parameter getParameter(){ 
      return parameter;
   }

   public String getComparison() {
      return comparison;
   }
   
   @Override
   public String toString() {
      return expression;
   }
}
