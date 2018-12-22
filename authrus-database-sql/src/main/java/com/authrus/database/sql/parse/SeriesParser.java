package com.authrus.database.sql.parse;

import java.util.regex.Pattern;

public class SeriesParser {
   
   private final Pattern pattern;
   
   public SeriesParser() {
      this.pattern = Pattern.compile("\\s*,\\s*");
   }
   
   public String[] parse(String series) {
      String expression = series.trim();
      int length = expression.length();
      
      if(length == 0) {
         throw new IllegalArgumentException("Series was empty");
      }      
      return pattern.split(expression);
   }
}
