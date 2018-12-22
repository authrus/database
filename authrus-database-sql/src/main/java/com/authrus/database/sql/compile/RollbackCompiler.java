package com.authrus.database.sql.compile;

import java.util.Collections;
import java.util.Map;

import com.authrus.database.sql.Query;
import com.authrus.database.sql.Verb;

public class RollbackCompiler extends QueryCompiler {

   public RollbackCompiler() {
      this(Collections.EMPTY_MAP);
   }
   
   public RollbackCompiler(Map<String, String> translations) {
      this.translations = translations;
   }  
   
   @Override
   public String compile(Query query, Object[] list) throws Exception {
      Verb verb = query.getVerb();
      String source = query.getSource();
      String original = verb.getVerb();
      String replace = translate(original);
      
      if(replace == null) {
         throw new IllegalStateException("Verb conversion for '" + source + "' was null");
      }
      String table = query.getTable();

      if(table != null) {
         return replace + " on " + table;
      }
      return replace;
   } 
}
