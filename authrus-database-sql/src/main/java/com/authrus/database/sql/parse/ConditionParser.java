package com.authrus.database.sql.parse;

import com.authrus.database.common.parse.Parser;
import com.authrus.database.sql.Condition;
import com.authrus.database.sql.Parameter;
import com.authrus.database.sql.ParameterType;

public class ConditionParser extends Parser {

   private final Token operator;
   private final Token value;   
   private final Token name;

   public ConditionParser() {
      this.operator = new Token();
      this.value = new Token();      
      this.name = new Token();
   }

   public ConditionParser(String text) {
      this();
      parse(text);
   }   
  
   public String getSource() {
      return new String(source, 0, count);
   }
   
   public String getOperator() {
      return operator.getText();
   }
  
   public Condition getCondition() {
      Parameter parameter = getParameter();
      String expression = getSource();
      String operator = getOperator();
      
      return new Condition(parameter, operator, expression);
   }   
   
   public Parameter getParameter() {
      ParameterType type = value.getType();
      String column = name.getText(); // x ==:y      
      String variable = value.getText();
      String token = null;
         
      if(type == ParameterType.NAME) { //name<=:x
         variable = variable.substring(1);           
      }
      if(type == ParameterType.VALUE) { //name!='hello world'
         token = variable;
         variable = null;           
      }
      if(type == ParameterType.TOKEN) { // name==?
         variable = null;           
      }     
      return new Parameter(type, column, variable, token); // no value      
   }  

   @Override
   protected void init() {      
      operator.clear();
      name.clear();
      value.clear();
      off = 0;     
   }

   @Override
   protected void parse() {
      pack();      
      condition();
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

   private void condition() {
      digest(name);
      operator(operator);
      digest(value);
   }
   
   private void operator(Token token) {
      token.off = off;
      token.len = 0;

      if(skip("like")) {
         token.len = 4;
         
         while(off < count){
            char next = source[off];
         
            if(!space(next)){              
               break;
            } 
            off++;
         }
      } else {
         while(off < count){
            char next = source[off++];
         
            if(!operator(next)){
               off--;
               break;
            }
            token.len++;   
         }
      }
   }

   private void digest(Token token) {
      token.off = off;
      token.len = 0;

      while(off < count){
         char next = source[off++];
         
         if(quote(next)){
            token.off = off;
            
            while(off < count) {
               if(source[off++] == next) {
                  token.len--;
                  break;
               }
               token.len++;
            }
         } else if(space(next)) {
            break;
         } else if(operator(next)) {
            off--;
            break;
         } 
         token.len++;
      }
   }  
   
   private boolean operator(char ch) {
      switch(ch) {
      case '<': case '>':
      case '=': case '!':
      case '~':
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
      if(ch >= '0' && ch <= '0') {
         return true;
      }
      return false;
   }

   private class Token {

      public int off;
      public int len;

      public Token() {
         this(0, 0);
      }

      public Token(int off, int len) {
         this.off = off;
         this.len = len;
      }
      
      public ParameterType getType() {
         if(len > 0) {
            if(source[off] == '?') {
               return ParameterType.TOKEN;
            }
            if(source[off] == ':') {
               return ParameterType.NAME;
            }
            return ParameterType.VALUE;
         }
         return null;
      }
      
      public String getText() {
         if(len > 0) { 
            return new String(source,off,len);
         }
         return null;
      }
      
      public void clear() {
         off = 0;
         len = 0;
      }

      public String toString() {
         return getText();
      }
   }
}
