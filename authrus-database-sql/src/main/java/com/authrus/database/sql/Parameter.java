package com.authrus.database.sql;

public class Parameter {

   public final ParameterType type;
   public final String column;
   public final String name;
   public final String value;
   
   public Parameter(ParameterType type, String column, String name, String value) {
      this.column = column;
      this.name = name;
      this.value = value;
      this.type = type;
   }
   
   public ParameterType getType(){ 
      return type;
   }
   
   public String getColumn() {
      return column;
   }

   public String getName() {
      return name;
   }

   public String getValue() {
      return value;
   }
   
   @Override
   public String toString() {
      return type.expression(column, name, value);
   }
}
