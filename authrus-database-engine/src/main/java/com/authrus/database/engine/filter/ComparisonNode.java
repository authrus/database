package com.authrus.database.engine.filter;

import com.authrus.database.Column;
import com.authrus.database.engine.index.RowSeries;

public class ComparisonNode implements FilterNode {
   
   private final Comparable value;
   private final String operator;
   private final Column column;
   private final String name;
   
   public ComparisonNode(Column column, Comparable value, String operator) {
      this.name = column.getName();
      this.operator = operator;
      this.column = column;
      this.value = value;
   }

   @Override
   public RowSeries apply(RowSeries series) {
      if(operator.equals("==")) {
         return series.equalTo(column, value);
      }
      if(operator.equals("=")) {
         return series.equalTo(column, value);
      }      
      if(operator.equals("!=")) {
         return series.notEqualTo(column, value);
      } 
      if(operator.equals(">")) {
         return series.greaterThan(column, value);
      } 
      if(operator.equals("<")) {
         return series.lessThan(column, value);
      }
      if(operator.equals(">=")) {
         return series.greaterThanOrEqual(column, value);
      } 
      if(operator.equals("<=")) {
         return series.lessThanOrEqual(column, value);
      }
      if(operator.equals("=~")) {
         return series.like(column, value);
      }
      if(operator.equals("like")) {
         return series.like(column, value);
      }        
      throw new IllegalArgumentException("Unknown comparison operator '" + operator + "'");
   }

   @Override
   public String toString() {
      return String.format("%s %s %s", name, operator, value);
   }
}
