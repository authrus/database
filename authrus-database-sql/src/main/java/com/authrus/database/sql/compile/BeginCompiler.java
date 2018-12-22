package com.authrus.database.sql.compile;

import java.util.Collections;
import java.util.Map;

import com.authrus.database.sql.Query;
import com.authrus.database.sql.Verb;

public class BeginCompiler extends QueryCompiler {
   
   public BeginCompiler() {
      this(Collections.EMPTY_MAP);
   }
   
   public BeginCompiler(Map<String, String> translations) {
      this.translations = translations;
   }   

   @Override
   public String compile(Query query, Object[] list) throws Exception {
      Verb verb = query.getVerb();
      String name = query.getName();
      String table = query.getTable();
      String source = query.getSource();
      String original = verb.getVerb();
      String replace = translate(original);
      
      if(replace == null) {
         throw new IllegalStateException("Verb conversion for '" + source + "' was null");
      }
      StringBuilder builder = new StringBuilder();
      
      builder.append(replace);
      
      if(name != null) {
         builder.append(" ");
         builder.append(name);
      }
      if(table != null) {
         builder.append(" on ");
         builder.append(table);
      }
      return builder.toString();
   } 
}
