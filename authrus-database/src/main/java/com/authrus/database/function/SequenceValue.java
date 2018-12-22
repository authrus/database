package com.authrus.database.function;

import static com.authrus.database.data.DataType.BYTE;
import static com.authrus.database.data.DataType.INT;
import static com.authrus.database.data.DataType.LONG;
import static com.authrus.database.data.DataType.SHORT;
import static com.authrus.database.data.DataType.SYMBOL;
import static com.authrus.database.data.DataType.TEXT;
import static com.authrus.database.function.DefaultFunction.SEQUENCE;

import java.util.concurrent.atomic.AtomicLong;

import com.authrus.database.Column;
import com.authrus.database.data.DataType;

public class SequenceValue implements DefaultValue {

   private final AtomicLong sequence;
   private final String expression;
   
   public SequenceValue(String expression) {
      this.sequence = new AtomicLong();
      this.expression = expression;
   }
   
   @Override
   public Comparable getDefault(Column column, Comparable value) {
      if(value == null) {
         DataType type = column.getDataType();
         long next = sequence.getAndIncrement();
         
         if(type == LONG) {
            return next;
         }
         if(type == INT) {
            return (int)next;
         }
         if(type == SHORT) {
            return (short)next;
         }
         if(type == BYTE) {
            return (byte)next;
         }           
         if(type == TEXT) {
            return String.valueOf(next);
         }
         if(type == SYMBOL) {
            return String.valueOf(next);
         }         
         throw new IllegalStateException("Unable to convert sequence to " + type);
      }
      return value;
   }
   
   @Override
   public DefaultFunction getFunction() {
      return SEQUENCE;
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
