package com.authrus.database.sql.parse;

public class QueryError {
   
   private final QueryErrorType type;
   private final String source;

   public QueryError(QueryErrorType type, String source) {
      this.source = source;
      this.type = type;
   }
   
   public QueryErrorType getType() {
      return type;
   }
   
   public String getSource() {
      return source;
   }
   
   @Override
   public String toString() {
      return String.format("%s: %s", type, source);
   }
}
