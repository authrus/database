package com.authrus.database.function;

public enum DefaultFunction {
   IDENTITY,
   CURRENT_TIME,
   LITERAL,        
   SEQUENCE;
   
   public static DefaultFunction resolveFunction(String expression) {
      if(expression != null) {
         if(expression.equalsIgnoreCase("sequence")) {
            return SEQUENCE;
         }
         if(expression.equalsIgnoreCase("time")) {
            return CURRENT_TIME;
         }
         return LITERAL;      
      }             
      return IDENTITY;  
   }
   
   public static DefaultValue resolveValue(String expression) {
      if(expression != null) {
         if(expression.equalsIgnoreCase("sequence")) {
            return new SequenceValue(expression);
         }
         if(expression.equalsIgnoreCase("time")) {
            return new CurrentTimeValue(expression);
         }
         return new LiteralValue(expression);      
      }             
      return new IdentityValue(expression);      
   }
}
