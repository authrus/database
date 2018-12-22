package com.authrus.database.sql.compile;

import static com.authrus.database.data.DataType.BOOLEAN;
import static com.authrus.database.data.DataType.SYMBOL;
import static com.authrus.database.data.DataType.TEXT;
import static com.authrus.database.sql.ParameterType.VALUE;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.authrus.database.data.DataType;
import com.authrus.database.sql.Parameter;
import com.authrus.database.sql.ParameterType;
import com.authrus.database.sql.Query;
import com.authrus.database.sql.Verb;

public class UpdateCompiler extends SelectCompiler {
   
   public UpdateCompiler() {
      this(Collections.EMPTY_MAP);
   }
   
   public UpdateCompiler(Map<String, String> translations) {
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
      List<Parameter> parameters = query.getParameters();
      String table = query.getTable();
      int count = parameters.size();
      int index = 0;
      
      if(table == null) {
         throw new IllegalArgumentException("Update statement '" + source + "' does not specify a table");
      }
      StringBuilder builder = new StringBuilder();
     
      builder.append(replace);
      builder.append(" ");
      builder.append(table);
      builder.append(" set ");
      
      for(int i = 0; i < count; i++) {
         Parameter parameter = parameters.get(i);
         ParameterType type = parameter.getType();
         String column = parameter.getColumn();
         Object value = parameter.getValue();
         
         if(type != VALUE) {
            value = list[index++];
         }            
         if(i > 0) {
            builder.append(", ");
         }         
         builder.append(column);
         builder.append(" = ");
         
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
      Object[] remainder = Arrays.copyOfRange(list, index, list.length);
      String whereClause = compileWhere(query, remainder);
      
      if(whereClause != null) {
         builder.append(" ");
         builder.append(whereClause);
      }
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
}
