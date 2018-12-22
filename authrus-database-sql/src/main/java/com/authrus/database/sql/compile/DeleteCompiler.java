package com.authrus.database.sql.compile;

import java.util.Collections;
import java.util.Map;

import com.authrus.database.sql.Query;
import com.authrus.database.sql.Verb;

public class DeleteCompiler extends SelectCompiler {
   
   public DeleteCompiler() {
      this(Collections.EMPTY_MAP);
   }
   
   public DeleteCompiler(Map<String, String> translations) {
      this.translations = translations;
   }  
   
   @Override
   public String compile(Query query, Object[] list) throws Exception {
      Verb verb = query.getVerb();
      String table = query.getTable();
      String source = query.getSource();   
      String original = verb.getVerb();
      String replace = translate(original);   
      
      if(replace == null) {
         throw new IllegalStateException("Verb conversion for '" + source + "' was null");
      } 
      if(table == null) {
         throw new IllegalArgumentException("Drop table statement '" + source + "' does not specify a table");
      }
      String whereClause = compileWhere(query, list);
      
      if(whereClause != null) {
         return replace + " from " + table + " " + whereClause;
      }
      return replace + " from " + table; 
   }

}
