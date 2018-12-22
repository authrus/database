package com.authrus.database.sql.parse;

import static com.authrus.database.sql.parse.QueryErrorType.BAD_QUOTED_STRING;
import static com.authrus.database.sql.parse.QueryErrorType.ILLEGAL_TOKEN;
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
import static com.authrus.database.sql.parse.QueryTokenType.INSERT_OR_IGNORE_VERB;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.authrus.database.common.parse.Parser;

public class QueryLexicalAnalyzer extends Parser {
   
   private final AtomicReference<QueryToken> last;
   private final List<QueryError> errors;
   private final List<QueryToken> tokens;
   private final AtomicBoolean fail;
   private final Token line;

   public QueryLexicalAnalyzer() {
      this.last = new AtomicReference<QueryToken>();
      this.tokens = new ArrayList<QueryToken>();
      this.errors = new ArrayList<QueryError>();
      this.fail = new AtomicBoolean();
      this.line = new Token();
   }

   public QueryLexicalAnalyzer(String text) {
      this();
      parse(text);
   }
   
   public boolean isSuccess() {
      return !fail.get();
   }
   
   public String getSource() {
      return new String(source, 0, count);
   }
   
   public List<QueryError> getErrors() {
      return errors;
   }
   
   public List<QueryToken> getTokens() {
      return tokens;
   }
   
   @Override
   protected void init() {
      errors.clear();
      line.clear();
      tokens.clear();
      fail.set(false);
      last.set(null);
      off = 0;
   }   

   @Override
   protected void parse() {
      pack();
      extract();
   }
   
   private void pack() {
      int pos = 0;

      while(off < count){
         if(quote(source[off])){
            char open = source[off];

            while(off < count) {
               source[pos++] = source[off++];

               if(off >= count) {
                  break;
               }
               if(source[off] == open) {
                  source[pos++] = source[off++];
                  break;
               }
               if(off >= count) {
                  error(BAD_QUOTED_STRING, 0);
               }
            }
         } else if(!space(source[off])) {
            source[pos++] = source[off++];
         } else {
            if(pos > 0 && off + 1< count) {
               char previous = source[pos-1];
               char current = source[off+1];
               
               if(identifier(previous) && identifier(current)) {
                  source[pos++] = ' ';               
               }
            }
            off++;
         }
      }
      count = pos;
      off = 0;
   } 

   public void extract(){      
      while(off < count) {
         if(consume("select distinct", true)) {
            digest(SELECT_DISTINCT_VERB);
         } else if(consume("select", true)) {
            digest(SELECT_VERB); 
         } else if(consume("update", true)) {
            digest(UPDATE_VERB);            
         } else if(consume("delete from", true)) {
            digest(DELETE_VERB);
         } else if(consume("insert or ignore into", true)) {
            digest(INSERT_OR_IGNORE_VERB);             
         } else if(consume("insert into", true)) {
            digest(INSERT_VERB);
         } else if(consume("drop table if exists", true)) {
            digest(DROP_TABLE_VERB); 
         } else if(consume("drop index if exists", true)) {            
            digest(DROP_INDEX_VERB);              
         } else if(consume("drop table", true)) {
            digest(DROP_TABLE_VERB); 
         } else if(consume("drop index", true)) {            
            digest(DROP_INDEX_VERB);        
         } else if(consume("create table if not exists", true)) {
            digest(CREATE_TABLE_VERB);
         } else if(consume("create index if not exists", true)) {
            digest(CREATE_INDEX_VERB);              
         } else if(consume("create table", true)) {
            digest(CREATE_TABLE_VERB);
         } else if(consume("create index", true)) {
            digest(CREATE_INDEX_VERB); 
         } else if(consume("begin transaction", true)) {
            digest(BEGIN_VERB);                
         } else if(consume("begin", true)) {
            digest(BEGIN_VERB);
         } else if(consume("commit transaction", true)) {
            digest(COMMIT_VERB);              
         } else if(consume("commit", true)) {
            digest(COMMIT_VERB);
         } else if(consume("rollback transaction", true)) {
            digest(ROLLBACK_VERB);             
         } else if(consume("rollback", true)) {
            digest(ROLLBACK_VERB); 
         } else if(consume("not null", true)) {
            digest(NOT_NULL);
         } else if(consume("default", true)) {
            digest(DEFAULT);            
         } else if(consume("primary key", true)) {
            digest(PRIMARY_KEY);
         } else if(consume("int", true)) {
            digest(INT);
         } else if(consume("text", true)) {
            digest(TEXT);
         } else if(consume("symbol", true)) {
            digest(SYMBOL);              
         } else if(consume("double", true)) {
            digest(DOUBLE);            
         } else if(consume("float", true)) { 
            digest(FLOAT);            
         } else if(consume("long", true)) { 
            digest(LONG);
         } else if(consume("short", true)) {
            digest(SHORT);
         } else if(consume("byte", true)) {
            digest(BYTE);
         } else if(consume("boolean", true)) { 
            digest(BOOLEAN);
         } else if(consume("char", true)) { 
            digest(CHAR);
         } else if(consume("date", true)) {
            digest(DATE);                                               
         } else if(consume("from", true)) {
            digest(FROM);
         } else if(consume("values", true)) {
            digest(VALUES);
         } else if(consume("set", true)) {
            digest(SET);             
         } else if(consume("where", true)) {
            digest(WHERE);
         } else if(consume("order by", true)) {
            digest(ORDER);
         } else if(consume("asc", true)) {
            digest(ASCENDING);
         } else if(consume("desc", true)) {
            digest(DESCENDING);  
         } else if(consume("limit", true)) {
            digest(LIMIT);             
         } else if(consume("and", true)) {
            digest(AND);
         } else if(consume("or", true)) {
            digest(OR);
         } else if(consume("not", true)) { /* is this a token or an operator */
            digest(NOT);
         } else if(consume("like", true)) {
            digest(LIKE);             
         } else if(consume("on", true)) {
            digest(ON);            
         } else if(consume("count(*)")) {
            digest(COUNT);              
         } else if(consume("*")) {
            digest(WILD);
         } else if(consume("(")){
            digest(OPEN);
         } else if(consume(")")){
            digest(CLOSE);
         } else if(consume(",")){
            digest(COMMA);              
         } else {
            digest(EXPRESSION);
         }
      }      
   }
   
   private void error(QueryErrorType type, int off) {
      String text = new String(source, 0, off);
      QueryError error = new QueryError(type, text);
      
      fail.set(true);
      errors.add(error);
      
   }
   
   private void save(QueryTokenType type) {
      QueryToken token = new QueryToken(type, source, line.off, line.len);      
      
      if(!tokens.isEmpty()) {
         QueryToken previous = last.get();
         
         if(!type.legal(token, previous)) {            
            error(ILLEGAL_TOKEN, off);
         }
      }
      last.set(token);
      tokens.add(token);
      line.clear();
   }
   
   private boolean consume(String token) {
      return consume(token, false);
   }
   
   private boolean consume(String token, boolean whole) {
      int length = token.length();
      int last = off + length;
      
      while(off < count) {
         if(!space(source[off])) {
            break;
         }
         off++;         
      }
      if(last < count && whole) {
         if(word(source[last])) {
            return false;
         }
      }
      if(skip(token)) {
         while(off < count) {
            if(!space(source[off])) {
               break;
            }
            off++;         
         }
         return true;
      }      
      return false;
   }
   
   private void digest(QueryTokenType type) {
      line.off = off;
      
      while(off < count) {
         char next = source[off];         
         
         if(type.terminal(source, off++, count)) {
            off--;
            break;
         } else {
            if(quote(next)){
               while(off < count) {
                  if(source[off++] == next) {
                     line.len++;                     
                     break;
                  }  
                  line.len++;
               }
            }
         }
         line.len++;
      }  
      save(type);
   }
   
   private boolean word(char ch) {
      if(ch >= 'a' && ch <= 'z') {
         return true;
      }
      if(ch >= 'A' && ch <= 'Z') {
         return true;
      }
      if(ch >= '0' && ch <= '0') {
         return true;
      }
      return false;
   } 
   
   private boolean identifier(char ch) {
      if(ch >= 'a' && ch <= 'z') {
         return true;
      }
      if(ch >= 'A' && ch <= 'Z') {
         return true;
      }
      if(ch >= '0' && ch <= '9') {
         return true;
      }
      if(ch == '\'' || ch == '"') {
         return true;
      }
      return ch == '?' || ch == '_';
   }  
   
   private class Token {

      public String cache;
      public int off;
      public int len;

      public Token() {
         this(0, 0);
      }

      public Token(int off, int len) {
         this.off = off;
         this.len = len;
      }

      public void clear() {
         cache = null;
         len = 0;
      }

      public String toString() {
         if(cache != null) {
            return cache;
         }
         if(len > 0) {
            cache = new String(source,off,len);
         }
         return cache;
      }
   }
}
