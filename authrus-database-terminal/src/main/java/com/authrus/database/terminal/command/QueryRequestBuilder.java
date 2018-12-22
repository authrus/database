package com.authrus.database.terminal.command;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.authrus.database.sql.Query;
import com.authrus.database.sql.parse.QueryParser;

public class QueryRequestBuilder {
   
   private static final String REPEAT_PATTERN = "\\s*repeat\\s+(\\d+)\\s+(.*)\\s*";
   
   private final QueryParser parser;

   public QueryRequestBuilder() {
      this.parser = new QueryParser();
   }   

   public QueryRequest createRequest(String source) {      
      try {
         String line = source.replaceAll("\\s+", " ");
         String expression = line.trim();              
         Pattern pattern = Pattern.compile(REPEAT_PATTERN, Pattern.CASE_INSENSITIVE);
         Matcher matcher = pattern.matcher(expression);
         
         if(matcher.matches()) {
            String token = matcher.group(1);
            String statement = matcher.group(2);
            Query query = parser.parse(statement);
            int repeat = Integer.parseInt(token);
            
            return new QueryRequest(query, expression, repeat);
         }
         Query query = parser.parse(expression);
         int repeat = 1;
         
         return new QueryRequest(query, expression, repeat);
      } catch(Exception e) {
         throw new IllegalStateException("Could not parse '" + source + "'", e);
      }
   }
}
