package com.authrus.database.sql.build;

import static com.authrus.database.sql.parse.QueryTokenType.AND;
import static com.authrus.database.sql.parse.QueryTokenType.ASCENDING;
import static com.authrus.database.sql.parse.QueryTokenType.BEGIN_VERB;
import static com.authrus.database.sql.parse.QueryTokenType.BOOLEAN;
import static com.authrus.database.sql.parse.QueryTokenType.BYTE;
import static com.authrus.database.sql.parse.QueryTokenType.CHAR;
import static com.authrus.database.sql.parse.QueryTokenType.CLOSE;
import static com.authrus.database.sql.parse.QueryTokenType.COMMA;
import static com.authrus.database.sql.parse.QueryTokenType.COMMIT_VERB;
import static com.authrus.database.sql.parse.QueryTokenType.COUNT;
import static com.authrus.database.sql.parse.QueryTokenType.CREATE_INDEX_VERB;
import static com.authrus.database.sql.parse.QueryTokenType.CREATE_TABLE_VERB;
import static com.authrus.database.sql.parse.QueryTokenType.DATE;
import static com.authrus.database.sql.parse.QueryTokenType.DEFAULT;
import static com.authrus.database.sql.parse.QueryTokenType.DELETE_VERB;
import static com.authrus.database.sql.parse.QueryTokenType.DESCENDING;
import static com.authrus.database.sql.parse.QueryTokenType.DOUBLE;
import static com.authrus.database.sql.parse.QueryTokenType.DROP_INDEX_VERB;
import static com.authrus.database.sql.parse.QueryTokenType.DROP_TABLE_VERB;
import static com.authrus.database.sql.parse.QueryTokenType.EXPRESSION;
import static com.authrus.database.sql.parse.QueryTokenType.FLOAT;
import static com.authrus.database.sql.parse.QueryTokenType.FROM;
import static com.authrus.database.sql.parse.QueryTokenType.INSERT_VERB;
import static com.authrus.database.sql.parse.QueryTokenType.INT;
import static com.authrus.database.sql.parse.QueryTokenType.LIKE;
import static com.authrus.database.sql.parse.QueryTokenType.LIMIT;
import static com.authrus.database.sql.parse.QueryTokenType.LONG;
import static com.authrus.database.sql.parse.QueryTokenType.NOT;
import static com.authrus.database.sql.parse.QueryTokenType.NOT_NULL;
import static com.authrus.database.sql.parse.QueryTokenType.ON;
import static com.authrus.database.sql.parse.QueryTokenType.OPEN;
import static com.authrus.database.sql.parse.QueryTokenType.OR;
import static com.authrus.database.sql.parse.QueryTokenType.ORDER;
import static com.authrus.database.sql.parse.QueryTokenType.PRIMARY_KEY;
import static com.authrus.database.sql.parse.QueryTokenType.ROLLBACK_VERB;
import static com.authrus.database.sql.parse.QueryTokenType.SELECT_DISTINCT_VERB;
import static com.authrus.database.sql.parse.QueryTokenType.SELECT_VERB;
import static com.authrus.database.sql.parse.QueryTokenType.SET;
import static com.authrus.database.sql.parse.QueryTokenType.SHORT;
import static com.authrus.database.sql.parse.QueryTokenType.SYMBOL;
import static com.authrus.database.sql.parse.QueryTokenType.TEXT;
import static com.authrus.database.sql.parse.QueryTokenType.UPDATE_VERB;
import static com.authrus.database.sql.parse.QueryTokenType.VALUES;
import static com.authrus.database.sql.parse.QueryTokenType.WHERE;
import static com.authrus.database.sql.parse.QueryTokenType.WILD;

import com.authrus.database.sql.parse.QueryToken;
import com.authrus.database.sql.parse.QueryTokenType;

public enum QueryPart {
   VERB(SELECT_VERB, SELECT_DISTINCT_VERB, DELETE_VERB, UPDATE_VERB, INSERT_VERB, CREATE_TABLE_VERB, CREATE_INDEX_VERB, DROP_TABLE_VERB, DROP_INDEX_VERB, BEGIN_VERB, COMMIT_VERB, ROLLBACK_VERB),
   COLUMNS(EXPRESSION, COMMA, OPEN, CLOSE, WILD, COUNT), 
   TYPES(EXPRESSION, INT, TEXT, SYMBOL, DOUBLE, FLOAT, LONG, SHORT, BYTE, BOOLEAN, CHAR, DATE, DEFAULT, NOT_NULL, COMMA, OPEN, CLOSE),
   KEY(PRIMARY_KEY, EXPRESSION, COMMA, OPEN, CLOSE),
   NAME(EXPRESSION),    
   PARAMETERS(EXPRESSION, VALUES, SELECT_VERB, SET, COMMA, OPEN, CLOSE),
   TABLE(FROM, ON, EXPRESSION),   
   WHERE_CLAUSE(WHERE, AND, OR, NOT, LIKE, EXPRESSION),
   ROW_LIMIT(LIMIT, EXPRESSION),   
   ORDER_BY(ORDER, EXPRESSION, ASCENDING, DESCENDING),
   EVERYTHING;
   
   private final QueryTokenType[] tokens;
   
   private QueryPart(QueryTokenType... tokens) {
      this.tokens = tokens;
   }
   
   public boolean accept(QueryToken token) {
      QueryTokenType type = token.getType();
      
      for(QueryTokenType entry : tokens) {
         if(entry == type) {
            return true;
         }
      }
      return false;
   }
} 
