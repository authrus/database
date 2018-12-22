package com.authrus.database.sql.compile;

import java.util.Collections;
import java.util.Map;

import com.authrus.database.sql.Query;
import com.authrus.database.sql.Verb;

public class DropIndexCompiler extends QueryCompiler {
   
   public DropIndexCompiler() {
      this(Collections.EMPTY_MAP);
   }
   
   public DropIndexCompiler(Map<String, String> translations) {
      this.translations = translations;
   }  

   @Override
   public String compile(Query query, Object[] list) throws Exception {
      Verb verb = query.getVerb();
      String name = query.getName();
      String source = query.getSource();   
      String original = verb.getVerb();
      String replace = translate(original);   
      
      if(replace == null) {
         throw new IllegalStateException("Verb conversion for '" + source + "' was null");
      }       
      if(name == null) {
         throw new IllegalArgumentException("Drop index statement '" + source + "' does not specify a name");
      }
      return replace + " " + name;      
   } 
}
