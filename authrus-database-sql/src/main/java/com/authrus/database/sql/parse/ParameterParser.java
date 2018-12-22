package com.authrus.database.sql.parse;

import java.util.ArrayList;
import java.util.List;

import com.authrus.database.common.parse.Parser;
import com.authrus.database.sql.Parameter;
import com.authrus.database.sql.ParameterType;

public class ParameterParser extends Parser {

   private final List<Parameter> parameters;
   private final Token value;   
   private final Token name;

   public ParameterParser() {
      this.parameters = new ArrayList<Parameter>();
      this.value = new Token();      
      this.name = new Token();
   }

   public ParameterParser(String text) {
      this();
      parse(text);
   }   
  
   public String getSource() {
      return new String(source, 0, count);
   }
  
   public List<Parameter> getParameters() {
      return new ArrayList<Parameter>(parameters);
   }
   
   public boolean isEmpty() {
      return parameters.isEmpty();
   }

   @Override
   protected void init() {
      parameters.clear();
      name.clear();
      value.clear();
      off = 0;     
   }

   @Override
   protected void parse() {
      pack();      
      parameters();
   }
   
   protected boolean isAssignment() {
      return false;
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

   private void parameters() {
      while(off < count) {
         parameter();
         insert();
         reset();
      }
   }

   private void parameter() {
      digest(name);
      
      if(skip("=")) {
         digest(value);
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
         } else if(separator(next)) {
            off--;
            break;
         } else if(terminal(next)) {
            break;
         } 
         token.len++;
      }
   }

   private void reset() {
      name.clear();
      value.clear();
   }

   private void insert() {
      Parameter parameter = create();
      
      if(parameter != null) {
         parameters.add(parameter);
      }      
   }
   
   private Parameter create() {
      ParameterType type = name.getType();
      String column = name.getText(); // x =:y      
      String variable = value.getText();
      String token = null;
      
      if(variable == null) {         
         if(type == ParameterType.NAME) { //:x
            variable = column.substring(1);
            column = null;
         }
         if(type == ParameterType.VALUE) { //'hello world'
            token = column;
            column = null;
         }
         if(type == ParameterType.TOKEN) { // ?
            variable = null;
            column = null;
         }
      } else {
         type = value.getType();
         
         if(type == ParameterType.NAME) { //name=:x
            variable = variable.substring(1);           
         }
         if(type == ParameterType.VALUE) { //name='hello world'
            token = variable;
            variable = null;           
         }
         if(type == ParameterType.TOKEN) { // name=?
            variable = null;           
         }
      } 
      return new Parameter(type, column, variable, token); // no value      
   }   
   
   private boolean separator(char ch) {
      return ch == '=';
   }
   
   private boolean terminal(char ch) {
      return ch == ',';
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
