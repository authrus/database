package com.authrus.database.sql.compile;

import static com.authrus.database.data.DataType.SYMBOL;
import static com.authrus.database.data.DataType.TEXT;
import static com.authrus.database.sql.ParameterType.VALUE;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.authrus.database.data.DataType;
import com.authrus.database.sql.Condition;
import com.authrus.database.sql.OrderByClause;
import com.authrus.database.sql.Parameter;
import com.authrus.database.sql.ParameterType;
import com.authrus.database.sql.Query;
import com.authrus.database.sql.Verb;
import com.authrus.database.sql.WhereClause;

public class SelectCompiler extends QueryCompiler {  
   
   private final boolean distinct;
   
   public SelectCompiler() {
      this(Collections.EMPTY_MAP);
   }
   
   public SelectCompiler(Map<String, String> translations) {
      this(translations, false);
   }
   
   public SelectCompiler(Map<String, String> translations, boolean distinct) {
      this.translations = translations;
      this.distinct = distinct;
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
      List<String> columns = query.getColumns();
      String table = query.getTable();
      int count = columns.size();
      
      if(table == null) {
         throw new IllegalArgumentException("Select statement '" + source + "' does not specify a table");
      }
      StringBuilder builder = new StringBuilder();
      
      builder.append(replace);
      builder.append(" ");
      
      if(count > 0) {
         for(int i = 0; i < count; i++) {
            String column = columns.get(i);
            
            if(i > 0) {
               builder.append(", ");            
            }
            builder.append(column);
         }      
      } else {
         builder.append("*");
      }
      builder.append(" from ");
      builder.append(table);
      
      String whereClause = compileWhere(query, list);
      String orderByClause = compileOrderBy(query, list);
      
      if(whereClause != null) {
         builder.append(" ");
         builder.append(whereClause);
      }
      if(orderByClause != null) {
         builder.append(" ");
         builder.append(orderByClause);
      }
      int limit = query.getLimit();      
      
      if(limit > 0) {
         builder.append(" limit ");
         builder.append(limit);
      }
      return builder.toString();    
   }   
   
   protected String compileWhere(Query query, Object[] list) throws Exception {
      WhereClause clause = query.getWhereClause();
      List<Condition> conditions = clause.getConditions();
      List<String> operators = clause.getOperators();
      int count = conditions.size();
      
      if(count > 0) {
         StringBuilder builder = new StringBuilder();
         int index = 0;
         
         builder.append("where ");
         
         for(int i = 0; i < count; i++) {
            Condition condition = conditions.get(i);
            Parameter parameter = condition.getParameter();
            ParameterType parameterType = parameter.getType();
            String column = parameter.getColumn();
            String comparison = condition.getComparison();
            Object value = parameter.getValue();
            
            if(parameterType != VALUE) {
               value = list[index++];
            }            
            if(i > 0) {
               builder.append(" ");
            }
            builder.append(column);
            builder.append(" ");
            
            if(comparison.equals("==")) {
               builder.append("=");
            } else {
               builder.append(comparison);
            }
            builder.append(" ");
            
            if(value != null) {
               Class require = value.getClass();
               DataType data = DataType.resolveType(require);
               
               if(data == TEXT || data == SYMBOL) {
                  String text = String.valueOf(value);
                  String quote = quote(text);
                  
                  builder.append(quote);                                 
               } else {
                  builder.append(value);
               }
            } else {
               builder.append("null");
            }
            if(i + 1 < count) {
               String operator = operators.get(i);
               
               builder.append(" ");
               builder.append(operator);               
            }
         }
         return builder.toString();
      }
      return null;
   }
   
   protected String compileOrderBy(Query query, Object[] list) throws Exception {
      OrderByClause clause = query.getOrderByClause();
      String expression = clause.getClause();
      
      if(expression != null) {
         int length = expression.length();
         
         if(length > 0) {
            StringBuilder builder = new StringBuilder();
            
            builder.append("order by ");
            builder.append(expression);
            
            return builder.toString();
         }
      }
      return null;
   }
}
