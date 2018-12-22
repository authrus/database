package com.authrus.database.sql;

public class OrderByClause {

   private final String clause;
   private final String column;
   private final String direction;

   public OrderByClause(String clause, String column) {
      this(clause, column, null);
   }
   
   public OrderByClause(String clause, String column, String direction) {
      this.direction = direction;
      this.column = column;
      this.clause = clause;
   }
   
   public String getDirection() {
      return direction;
   }
   
   public String getColumn() {
      return column;
   }
   
   public String getClause() {
      return clause;
   }
   
   @Override
   public String toString() {
      return clause;
   }
}
