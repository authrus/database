package com.authrus.database.function;

import static com.authrus.database.function.DefaultFunction.LITERAL;

import com.authrus.database.Column;
import com.authrus.database.common.StringConverter;
import com.authrus.database.data.DataType;

public class LiteralValue implements DefaultValue {   
   
   private final String expression;
   
   public LiteralValue(String expression) {
      this.expression = expression;
   }

   @Override
   public Comparable getDefault(Column column, Comparable value) {
      if(value == null) {
         DataType data = column.getDataType();
         Class type = data.getType();
         
         try {
            return (Comparable)StringConverter.convert(type, expression);                   
         } catch(Exception e) {
            throw new IllegalArgumentException("Unable to convert '" + expression + "' to " + data);
         }
      }
      return value;
   }

   @Override
   public DefaultFunction getFunction() {
      return LITERAL;
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
