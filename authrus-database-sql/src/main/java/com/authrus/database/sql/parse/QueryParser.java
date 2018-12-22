package com.authrus.database.sql.parse;

import java.util.List;

import com.authrus.database.common.collection.Cache;
import com.authrus.database.common.collection.LeastRecentlyUsedCache;
import com.authrus.database.sql.Query;
import com.authrus.database.sql.build.QueryBuilder;

public class QueryParser {

   private final QueryLexicalAnalyzer analyzer;
   private final Cache<String, Query> cache;
   
   public QueryParser() {
      this(100);
   }
   
   public QueryParser(int capacity) {   
      this.cache = new LeastRecentlyUsedCache<String, Query>(capacity);
      this.analyzer = new QueryLexicalAnalyzer();
   }

   public synchronized Query parse(String text) {
      if(!cache.contains(text)) {
         analyzer.parse(text);
         
         if(!analyzer.isSuccess()) {
            List<QueryError> errors = analyzer.getErrors();
            
            if(!errors.isEmpty()) {
               throw new IllegalStateException("Query '" + text + "' could not be parsed " + errors);
            }
         }
         Query command = create(text);
            
         if(command != null) {
            cache.cache(text, command);
         }         
      }
      return cache.fetch(text);
   }
   
   private synchronized Query create(String text) {
      List<QueryToken> tokens = analyzer.getTokens();
      
      if(!tokens.isEmpty()) {
         QueryBuilder builder = new QueryBuilder();
         
         for(QueryToken token : tokens) {
            builder.update(token);
         }
         return builder.createCommand(text);         
      }   
      return null;
   }

}
