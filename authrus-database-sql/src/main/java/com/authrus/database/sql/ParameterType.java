package com.authrus.database.sql;

public enum ParameterType {
   VALUE {
      @Override
      public String expression(String column, String name, String value) {
         if(column != null) {
            return column + "=" + value;
         }
         return value;
      }
   },    
   TOKEN {
      @Override
      public String expression(String column, String name, String value) {
         if(column != null) {
            return column + "=?";
         }
         return "?";
      }
   },  
   NAME {
      @Override
      public String expression(String column, String name, String value) {
         if(column != null) {
            return column + "=:" + name;
         }
         return ":" + name;
      }
   };
   
   public abstract String expression(String column, String name, String value);
}
