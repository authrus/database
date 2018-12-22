package com.authrus.database.sql.build;

import com.authrus.database.sql.Query;
import com.authrus.database.sql.QueryConverter;
import com.authrus.database.sql.parse.QueryParser;

public class QueryProcessor<T> {

   private final QueryConverter<T> converter;
   private final QueryParser parser;
   
   public QueryProcessor(QueryConverter<T> converter) {
      this.parser = new QueryParser();
      this.converter = converter;
   }
   
   public T process(String text) {
      Query command = parser.parse(text);
      
      if(command != null) {
         return converter.convert(command);
      }
      return null;
   }
}
