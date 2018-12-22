package com.authrus.database.sql.parse;

public class QueryToken {

   private final QueryTokenType type;
   private final String text;
   private final int length;
   
   public QueryToken(QueryTokenType type, char[] source) {
      this(type, source, 0, source.length);
   }
   
   public QueryToken(QueryTokenType type, char[] source, int offset, int length) {
      this.text = new String(source, offset, length);
      this.length = length;
      this.type = type;
   }
   
   public QueryTokenType getType() {
      return type;
   }
   
   public String getToken() {
      return text;
   }

   public int getLength() {
      return length;
   }
   
   @Override
   public String toString() {
      return String.format("%s(%s)", type, text);
   }
}
