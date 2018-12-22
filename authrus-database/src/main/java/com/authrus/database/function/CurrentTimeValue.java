package com.authrus.database.function;

import static com.authrus.database.data.DataType.DATE;
import static com.authrus.database.data.DataType.LONG;
import static com.authrus.database.data.DataType.SYMBOL;
import static com.authrus.database.data.DataType.TEXT;
import static com.authrus.database.function.DefaultFunction.CURRENT_TIME;

import com.authrus.database.Column;
import com.authrus.database.data.DataType;
import com.authrus.database.data.DateParser;

public class CurrentTimeValue implements DefaultValue {
   
   private final String expression;
   
   public CurrentTimeValue(String expression) {
      this.expression = expression;
   }

   @Override
   public Comparable getDefault(Column column, Comparable value) {
      if(value == null) {
         DataType type = column.getDataType();
         long time = System.currentTimeMillis();
         
         if(type == DATE) {
            return DateParser.toDate(time);
         }
         if(type == TEXT) {
            return DateParser.toString(time);
         }
         if(type == SYMBOL) {
            return DateParser.toString(time);
         }         
         if(type == LONG) {
            return time;
         }         
         throw new IllegalStateException("Unable to convert time to " + type);
      }
      return value;
   }
   
   @Override
   public DefaultFunction getFunction() {
      return CURRENT_TIME;
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
