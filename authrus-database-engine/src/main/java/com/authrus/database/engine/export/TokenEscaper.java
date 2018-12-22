package com.authrus.database.engine.export;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TokenEscaper {
   
   private final Map<Character, String> escapes;
   private final List<Character> codes;
   private final StringBuilder builder;
   
   public TokenEscaper(Map<Character, String> escapes) {
      this.codes = new ArrayList<Character>();
      this.builder = new StringBuilder();
      this.escapes = escapes;
   }
   
   public synchronized String escape(String value) {
      int required = escapes.size();
      int actual = codes.size();
      
      if(required != actual) {
         Set<Character> keys = escapes.keySet();
         
         for(Character key : keys) {
            codes.add(key);
         }
      }
      if(value != null) {
         for(Character code : codes) {
            int index = value.indexOf(code);
            
            if(index != -1) {
               return convert(value);
            }
         }
      }
      return value;
   }
   
   private synchronized String convert(String value) {
      int length = value.length();

      for(int i = 0; i < length; i++) {
         Character code = value.charAt(i);
         String token = escapes.get(code);
         
         if(token != null) {
            builder.append(token);
         } else {
            builder.append(code);
         }         
      }
      String result = builder.toString();
      
      if(length > 0) {
         builder.setLength(0);
      }
      return result;
   }   
}
