package com.authrus.database.sql.parse;

public enum QueryTokenType {
   SELECT_VERB("select", "count(*)", "(", "*", "expression") {
      public boolean terminal(char[] source, int off, int count) {
         return source[off - 1] == ' ' || source[off] == '(' || source[off] == '*' || source[off] == ')'; 
      }
   },
   SELECT_DISTINCT_VERB("select distinct", "(", "*", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return source[off - 1] == ' ' || source[off] == '(' || source[off] == '*';
      }
   },
   INSERT_VERB("insert into", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return source[off - 1] == ' ' || source[off] == '(';
      }
   },
   INSERT_OR_IGNORE_VERB("insert or ignore into", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return source[off - 1] == ' ' || source[off] == '(';
      }
   },   
   UPDATE_VERB("update", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return source[off - 1] == ' ';
      }
   },
   DELETE_VERB("delete from", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return source[off - 1] == ' ';
      }
   },
   CREATE_TABLE_VERB("create table", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return source[off - 1] == ' ';
      }
   },
   CREATE_INDEX_VERB("create index", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return source[off - 1] == ' ';
      }
   },
   DROP_TABLE_VERB("drop table", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return source[off - 1] == ' ';
      }
   },
   DROP_INDEX_VERB("drop index", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return source[off - 1] == ' ';
      }
   },
   BEGIN_VERB("begin", "on", "expression") {
      public boolean terminal(char[] source, int off, int count) {
         return source[off - 1] == ' '; 
      }
   },
   COMMIT_VERB("commit", "on", "expression") {
      public boolean terminal(char[] source, int off, int count) {
         return source[off - 1] == ' '; 
      }
   },
   ROLLBACK_VERB("rollback", "on", "expression") {
      public boolean terminal(char[] source, int off, int count) {
         return source[off - 1] == ' '; 
      }
   },   
   FROM("from", "(", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return true;
      }
   },   
   WHERE("where", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return true;
      }
   },
   AND("and", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return source[off - 1] == ' ';
      }
   },
   OR("or", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return source[off - 1] == ' ';
      }
   },
   NOT("not", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return source[off - 1] == ' ';
      }
   },
   IN("in", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return source[off - 1] == ' ';
      }
   },
   ON("on", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return source[off - 1] == ' ';
      }
   },   
   VALUES("values", "(", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return source[off] == '(' || source[off] == ' ';
      }
   },
   SET("set", "where", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return source[off] == '(' || source[off] == ' ';
      }
   },   
   ORDER("order by", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return source[off - 1] == ' ';
      }
   },
   ASCENDING("asc", "limit"){
      public boolean terminal(char[] source, int off, int count) {
         return source[off - 1] == ' ';
      }
   },
   DESCENDING("desc", "limit"){
      public boolean terminal(char[] source, int off, int count) {
         return source[off - 1] == ' ';
      }
   },
   COUNT("count(*)", "from", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return source[off - 1] == ')';
      }
   },
   NOT_NULL("not null", "default", ",", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return true;
      }
   },  
   DEFAULT("default", ",", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return true;
      }
   },    
   INT("int", "default", "not null", ",", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return true;
      }
   },
   TEXT("text", "default", "not null",  ",", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return true;
      }
   },
   SYMBOL("symbol", "default", "not null",  ",", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return true;
      }
   },     
   DOUBLE("double", "default", "not null", ",", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return true;
      }
   },    
   FLOAT("float", "default", "not null", ",", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return true;
      }
   },
   LONG("long", "default", "not null", ",", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return true;
      }
   },   
   SHORT("short", "default", "not null", ",", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return true;
      }
   },   
   BYTE("byte", "default", "not null", ",", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return true;
      }
   },
   BOOLEAN("boolean", "default", "not null", ",", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return true;
      }
   },   
   CHAR("char", "default", "not null", ",", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return true;
      }
   },   
   DATE("date", "default", "not null", ",", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return true;
      }
   },   
   PRIMARY_KEY("primary key", "(", ",", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return true;
      }
   },  
   LIMIT("limit", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return true;
      }
   },
   LIKE("like", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return true;
      }   
   },
   WILD("*", "from", "expression"){
      public boolean terminal(char[] source, int off, int count) {
         return true;
      }
   },
   OPEN("(", ")", ",", "expression"){
      public boolean terminal(char[] source, int off, int count) {        
         return true;
      }
   },
   CLOSE(")", "values", "select", "from", ")", ",", "expression"){
      public boolean terminal(char[] source, int off, int count) {        
         return source[off - 1] == ')';
      }
   },
   COMMA(",", "primary key", "expression"){
      public boolean terminal(char[] source, int off, int count) {        
         return true;
      }
   },    
   EXPRESSION("expression", "int", "double", "float", "text", "symbol", "long", "short", "byte", "boolean", "char", "date", "on", "like", "from", "where", "set", "values", "and", "or", "not", "like", "order by", "asc", "desc", "limit", "(", ")", ","){
      public boolean terminal(char[] source, int off, int count) {        
         return off == count || source[off] == ' ' || source[off] == '(' || source[off - 1] == '?' || source[off] == ')' || source[off] == ',';
      }
   };
   
   private final String[] follow;
   private final String name;
   
   private QueryTokenType(String name, String... follow) {
      this.follow = follow;
      this.name = name;      
   }
   
   public boolean legal(QueryToken current, QueryToken previous) {
      if(previous != null) {
         QueryTokenType after = current.getType();
         QueryTokenType before = previous.getType();
         
         for(String option : before.follow) {
            if(after.name.equals(option)) {
               return true;
            }
         }
         return false;
      }
      return true;
   }   
   
   public abstract boolean terminal(char[] source, int off, int count);
}
