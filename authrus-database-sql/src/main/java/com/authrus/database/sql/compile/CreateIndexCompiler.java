package com.authrus.database.sql.compile;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.authrus.database.sql.Query;
import com.authrus.database.sql.Verb;

public class CreateIndexCompiler extends QueryCompiler {
   
   public CreateIndexCompiler() {
      this(Collections.EMPTY_MAP);
   }
   
   public CreateIndexCompiler(Map<String, String> translations) {
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
      if(name == null) {
         throw new IllegalArgumentException("Create index statement '" + source + "' requires a name");
      }
      if(table == null) {
         throw new IllegalArgumentException("Create index statement '" + source + "' does not specify a table");
      }
      StringBuilder builder = new StringBuilder();
      
      builder.append(replace);
      builder.append(" ");
      builder.append(name);
      builder.append(" on ");
      builder.append(table);
      builder.append(" (");
      
      List<String> columns = query.getColumns();
      int count = columns.size();
      
      for(int i = 0; i < count; i++) {
         String column = columns.get(i);
         
         if(i > 0) {
            builder.append(", ");
         }
         builder.append(column);
      }
      builder.append(")");
      
      return builder.toString();      
   }    
}
