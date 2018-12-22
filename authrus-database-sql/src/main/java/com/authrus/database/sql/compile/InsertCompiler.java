package com.authrus.database.sql.compile;

import static com.authrus.database.data.DataType.BOOLEAN;
import static com.authrus.database.data.DataType.SYMBOL;
import static com.authrus.database.data.DataType.TEXT;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.authrus.database.data.DataType;
import com.authrus.database.sql.Query;
import com.authrus.database.sql.Verb;

public class InsertCompiler extends QueryCompiler {
   
   public InsertCompiler() {
      this(Collections.EMPTY_MAP);
   }
   
   public InsertCompiler(Map<String, String> translations) {
      this.translations = translations;
   }  
   
   @Override
   public String compile(Query query, Object[] list) throws Exception {
      Verb verb = query.getVerb();
      String source = query.getSource();
      String original = verb.getVerb();
      String table = query.getTable();
      String replace = translate(original);
      
      if(replace == null) {
         throw new IllegalStateException("Verb conversion for '" + source + "' was null");
      }
      if(table == null) {
         throw new IllegalArgumentException("Insert statement '" + source + "' does not specify a table");
      }
      StringBuilder builder = new StringBuilder();
      
      builder.append(replace);
      builder.append(" into ");
      builder.append(table);
      
      List<String> columns = query.getColumns();
      int count = columns.size();      
      
      if(count > 0) {         
         builder.append(" (");
         
         for(int i = 0; i < count; i++) {
            String column = columns.get(i);
            
            if(i > 0) {
               builder.append(", ");
            }         
            builder.append(column);
         }
         builder.append(")");
      }
      builder.append(" values (");
      
      for(int i = 0; i < list.length; i++) {
         Object value = list[i];
      
         if(i > 0) {
            builder.append(", ");
         } 
         if(value != null) {
            String token = compileToken(value);
            
            if(token == null) {
               throw new IllegalArgumentException("Translation of '" + value +"' was null in '" + source + "'");
            }
            builder.append(token);
         } else {
            builder.append("null");
         }
      }
      builder.append(")");

      return builder.toString();   
   }
   
   public String compileToken(Object value) throws Exception {
      Class type = value.getClass();
      DataType data = DataType.resolveType(type);
      String text = String.valueOf(value);      
      
      if(text.equalsIgnoreCase("null")) {
         return text;
      } 
      if(data == TEXT) {
         return quote(text);
      }
      if(data == SYMBOL) {
         return quote(text);
      }      
      if(data == BOOLEAN) {
         return quote(text);
      }      
      return text;
   }
   
   
   public String compileToken(Query query) throws Exception {
      Verb verb = query.getVerb();
      String value = verb.getVerb();
      
      if(value != null) {
         String key = value.toLowerCase();
         
         if(translations.containsKey(key)) {
            return translations.get(key);
         }
      }
      return value; 
   }
}
