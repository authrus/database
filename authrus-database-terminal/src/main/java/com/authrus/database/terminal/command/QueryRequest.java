package com.authrus.database.terminal.command;

import com.authrus.database.sql.Query;

public class QueryRequest {

   private final String expression;
   private final Query query;
   private final int repeat;

   public QueryRequest(Query query, String expression) {
      this(query, expression, 1);
   }
   
   public QueryRequest(Query query, String expression, int repeat) {
      this.query = query;
      this.repeat = repeat;
      this.expression = expression;
   }   
   
   public String getExpression() {
      return expression;
   }

   public Query getQuery() {
      return query;
   }

   public int getRepeat() {
      return repeat;
   }
   
   @Override
   public String toString() {
      return expression;
   }
}
