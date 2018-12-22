package com.authrus.database.function;

import static com.authrus.database.function.DefaultFunction.IDENTITY;

import com.authrus.database.Column;

public class IdentityValue implements DefaultValue {

   private final String expression;
   
   public IdentityValue(String expression) {
      this.expression = expression;
   }
   
   @Override
   public Comparable getDefault(Column column, Comparable value) {
      return value;
   }

   @Override
   public DefaultFunction getFunction() {
      return IDENTITY;
   }   

   @Override
   public String getExpression() {
      return expression;
   }       
   
   @Override
   public String toString() {
      return expression;
   }
}
