package com.authrus.database.sql.compile;

import static com.authrus.database.function.DefaultFunction.CURRENT_TIME;
import static com.authrus.database.function.DefaultFunction.LITERAL;
import static com.authrus.database.function.DefaultFunction.SEQUENCE;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.authrus.database.Column;
import com.authrus.database.PrimaryKey;
import com.authrus.database.Schema;
import com.authrus.database.data.DataConstraint;
import com.authrus.database.data.DataType;
import com.authrus.database.function.DefaultFunction;
import com.authrus.database.function.DefaultValue;
import com.authrus.database.sql.Query;
import com.authrus.database.sql.Verb;

public class CreateTableCompiler extends QueryCompiler {

   public CreateTableCompiler() {
      this(Collections.EMPTY_MAP);
   }
   
   public CreateTableCompiler(Map<String, String> translations) {
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
      Schema schema = query.getCreateSchema();
      String table = query.getTable();
      
      if(table == null) {
         throw new IllegalArgumentException("Create table statement '" + source + "' does not specify a table");
      }
      if(schema == null) {
         throw new IllegalArgumentException("Create table for '" + table + "' has no schema");
      }
      List<String> names = schema.getColumns();
      
      if(names.isEmpty()) {
         throw new IllegalArgumentException("Unable to create table '" + table + "' with no columns");
      }
      StringBuilder builder = new StringBuilder();
      Set<String> exists = new HashSet<String>();
      
      builder.append(replace);
      builder.append(" ");
      builder.append(table);
      builder.append(" (\n");
      
      for(String name : names) {
         Column column = schema.getColumn(name);
         String definition = compileColumn(column);
         
         builder.append("   ");
         builder.append(definition);
         builder.append(",\n");
         exists.add(name);
      }
      String key = compilePrimaryKey(query);
      
      if(key != null) {
         builder.append("   ");
         builder.append(key);
      }
      builder.append("\n)");
      
      return builder.toString();      
   }   
   
   public String compilePrimaryKey(Query query) throws Exception {
      Schema schema = query.getCreateSchema();
      PrimaryKey key = schema.getKey();
      List<String> keys = key.getColumns();
      int count = keys.size();
      
      if(keys.isEmpty()) {
         throw new IllegalArgumentException("Table has '" + count + "' key columns defined");
      }
      StringBuilder builder = new StringBuilder();
      
      builder.append("primary key (");
      
      for(int i = 0; i < count; i++) {
         String name = keys.get(i);
         
         if(i > 0) {
            builder.append(",");
         }
         builder.append(name);
      }
      builder.append(")");
      
      return builder.toString();
   }
   
   public String compileColumn(Column column) throws Exception {
      String constraint = compileConstraint(column);
      String substitute = compileDefault(column);
      String type = compileType(column);
      String title = column.getName();
      
      if(type == null) {
         throw new IllegalStateException("Type conversion for '" + column + "' was null");
      }
      StringBuilder builder = new StringBuilder();      
      
      builder.append(title);
      builder.append(" ");
      builder.append(type);
      
      if(constraint != null) {
         builder.append(" ");
         builder.append(constraint);
      }
      if(substitute != null) {
         builder.append(" ");
         builder.append(substitute);
      }
      return builder.toString();
   }
   
   public String compileType(Column column) throws Exception {
      DataType type = column.getDataType();
      String name = type.getName();
      
      if(name != null) { 
         return translate(name);
      }
      return name;     
   }
   
   public String compileConstraint(Column column) throws Exception {
      DataConstraint constraint = column.getDataConstraint();
      String name = constraint.getName();
      
      if(name != null) { 
         return translate(name);
      }
      return name; 
   }
   
   public String compileDefault(Column column) throws Exception {
      DefaultValue substitute = column.getDefaultValue();
      DefaultFunction function = substitute.getFunction();
      String value = substitute.getExpression();
      
      if(value != null) { 
         String result = translate(value);
         
         if(result != null) {
            if(function == SEQUENCE) {
               return result;
            }
            if(function == CURRENT_TIME) {
               return "default " + result;
            }
            if(function == LITERAL) {
               return "default " + result;
            }
         }
      }
      return value; 
   }
}
