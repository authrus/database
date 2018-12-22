package com.authrus.database.sql.build;

import static com.authrus.database.sql.parse.QueryTokenType.AND;
import static com.authrus.database.sql.parse.QueryTokenType.*;
import static com.authrus.database.sql.parse.QueryTokenType.NOT;
import static com.authrus.database.sql.parse.QueryTokenType.OR;
import static com.authrus.database.sql.parse.QueryTokenType.WHERE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.authrus.database.sql.Condition;
import com.authrus.database.sql.Parameter;
import com.authrus.database.sql.WhereClause;
import com.authrus.database.sql.parse.ConditionParser;
import com.authrus.database.sql.parse.QueryToken;
import com.authrus.database.sql.parse.QueryTokenType;

public class WhereClauseBuilder {
   
   private final AtomicReference<QueryTokenType> last;
   private final List<Condition> conditions;
   private final List<String> operators;
   private final StringBuilder builder;
   private final ConditionParser parser;
   private final StringBuilder clause;
   
   public WhereClauseBuilder() {
      this.last = new AtomicReference<QueryTokenType>();
      this.conditions = new ArrayList<Condition>();
      this.operators = new ArrayList<String>();
      this.parser = new ConditionParser();
      this.builder = new StringBuilder();
      this.clause = new StringBuilder();
   }
   
   public WhereClause createClause() {      
      String source = builder.toString();
      int length = source.length();
      
      if(length > 0) {
         parser.parse(source);
         builder.setLength(0);
         
         Condition condition = parser.getCondition();
         Parameter parameter = condition.getParameter();
         String comparison = condition.getComparison();
         String column = parameter.getColumn();
         
         if(column == null) {
            throw new IllegalStateException("Condition '" + source + "' did not have a column");
         }
         clause.append(column);
         clause.append(comparison);
         clause.append("?");
         conditions.add(condition); 
      }
      List<Condition> conditions = createConditions();
      List<String> operators = createOperators();
      String text = clause.toString();
      
      return new WhereClause(text, conditions, operators);
   }
   
   private List<String> createOperators() {
      int size = operators.size();     
      
      if(size > 0) {
         return Collections.unmodifiableList(operators);
      }
      return Collections.emptyList();
   }
   
   private List<Condition> createConditions() {
      int size = conditions.size();     
      
      if(size > 0) {
         return Collections.unmodifiableList(conditions);
      }
      return Collections.emptyList();
   }
   
   public void update(QueryToken token) {
      QueryTokenType type = token.getType();
      
      if(type == WHERE) {
         beginClause(token);
      } else if(type == EXPRESSION) {
         appendCondition(token);
      } else if(type == LIKE) {
         appendCondition(token);         
      } else if(type == AND){
         appendOperation(token);
      } else if(type == OR) {
         appendOperation(token);
      } else if(type == NOT) {
         appendOperation(token);
      } else {
         throw new IllegalStateException("Clause '" + clause + "' cannot process token '" + token + "'");
      }
      last.set(type);
   }
   
   private void beginClause(QueryToken token) {
      QueryTokenType previous = last.get();  
      
      if(previous != null) {
         throw new IllegalStateException("Clause '" + clause + "' has already started");
      }
   }
   
   private void appendCondition(QueryToken token) {
      QueryTokenType type = token.getType();      
      String text = token.getToken();
      
      if(type == LIKE) {         
         builder.append(" like ");         
      } else {      
         builder.append(text);
      }
   }
   
   private void appendOperation(QueryToken token) {
      QueryTokenType current = token.getType();
      QueryTokenType previous = last.get();          
   
      if(previous != EXPRESSION) {      
         throw new IllegalStateException("Clause '" + clause + "' has an out of sequence token '" + token + "'");
      }
      String source = builder.toString();
      int length = source.length();
      
      if(length <= 0) {
         throw new IllegalStateException("Clause '" + clause + "' is missing a condition");
      }
      parser.parse(source);
      builder.setLength(0);
      
      Condition condition = parser.getCondition();
      Parameter parameter = condition.getParameter();
      String comparison = condition.getComparison();
      String column = parameter.getColumn();
      
      if(column == null) {
         throw new IllegalStateException("Condition '" + source + "' did not have a column");
      }
      clause.append(column);
      clause.append(comparison);
      clause.append("?");
      conditions.add(condition); 
      
      if(current == AND) {
         operators.add("and");
         clause.append(" and ");
      } else if(current == OR) {
         operators.add("or");
         clause.append(" or "); 
      } else if(current == NOT) {
         operators.add("not");
         clause.append(" not ");
      } else {
         throw new IllegalStateException("Clause '" + clause + "' cannot accept token " + token);
      }
   }
   
   @Override
   public String toString() {
      return clause.toString();
   }

}
