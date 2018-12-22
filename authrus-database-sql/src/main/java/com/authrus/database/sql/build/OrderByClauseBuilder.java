package com.authrus.database.sql.build;

import static com.authrus.database.sql.parse.QueryTokenType.ASCENDING;
import static com.authrus.database.sql.parse.QueryTokenType.DESCENDING;
import static com.authrus.database.sql.parse.QueryTokenType.EXPRESSION;
import static com.authrus.database.sql.parse.QueryTokenType.ORDER;

import java.util.concurrent.atomic.AtomicReference;

import com.authrus.database.sql.OrderByClause;
import com.authrus.database.sql.parse.QueryToken;
import com.authrus.database.sql.parse.QueryTokenType;

public class OrderByClauseBuilder {
   
   private final AtomicReference<QueryTokenType> last;
   private final AtomicReference<String> direction;
   private final AtomicReference<String> column;
   private final StringBuilder clause;
   
   public OrderByClauseBuilder() {
      this.last = new AtomicReference<QueryTokenType>();
      this.direction = new AtomicReference<String>();
      this.column = new AtomicReference<String>();
      this.clause = new StringBuilder();
   }
   
   public OrderByClause createClause() {
      String text = clause.toString();
      String name = column.get();
      String sort = direction.get();
      
      return new OrderByClause(text, name, sort);
   }
   
   public void update(QueryToken token) {
      QueryTokenType type = token.getType();
      
      if(type == ORDER) {
         beginClause(token);
      } else if(type == EXPRESSION) {
         appendColumn(token);
      } else if(type == ASCENDING){
         appendDirection(token);
      } else if(type == DESCENDING) {
         appendDirection(token);
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
   
   private void appendColumn(QueryToken token) {
      String value = token.getToken();
      
      if(!column.compareAndSet(null, value)) {
         throw new IllegalStateException("Order clause '" + clause + "' cannot take a second column '" + value + "'");
      }
      clause.append(value);
   }
   
   private void appendDirection(QueryToken token) {     
      QueryTokenType type = token.getType();
      
      if(type == ASCENDING) {
         direction.set("asc");
         clause.append(" asc");
      } else if(type == DESCENDING) {
         direction.set("desc");
         clause.append(" desc");
      } else {
         throw new IllegalStateException("Order clause '" + clause + "' can only take 'asc' or 'desc' directions");
      }
   }
   
   @Override
   public String toString() {
      return clause.toString();
   }

}
