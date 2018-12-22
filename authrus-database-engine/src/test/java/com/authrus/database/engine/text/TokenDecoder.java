package com.authrus.database.engine.text;

public class TokenDecoder {   
   
   private static final char[] NULL = {'N', 'U', 'L', 'L'};
   
   private String expression;
   private char[] source;
   private int off;
   
   public TokenDecoder(String expression) {     
      this.source = expression.toCharArray();
      this.expression = expression;       
   }   
   
   public int count() {
      return source.length - off;
   }
   
   public char peek() {
      return source[off];
   }
   
   public char next() {
      return source[off++];
   }
   
   public boolean skip(char value) {
      char next = source[off];
      
      if(next == value) {
         off++;
         return true;
      }
      return false;
   }
   
   public boolean skip(String text) {
      int length = text.length();
    
      if(source.length - off >= length) {
         for(int i = 0; i < length; i++) {
            char value = text.charAt(i);
            char next = source[off + i];
            
            if(value != next) {
               return false;
            }
         }
         off += length;
         return true;
      }
      return false;
   }
   
   public Number number() {
      int mark = off;
      long value = 0;
      
      while(off < source.length) {
         char next = source[off];
         
         if(next > '9' || '0' > next) {
            if(off <= mark) {
               throw new IllegalStateException("Token at " + off + " is not numeric for '" + expression + "'");
            }
            return value;
         }
         value *= 10;
         value += next;
         value -= '0';  
         off++;
      }
      return value;            
   }
   
   public String text() {
      char next = source[off];
      int escape = 0;
      int length = 0;
      int mark = off;
      
      if(terminal(next)) {
         throw new IllegalStateException("Token at " + off + " is not text for '" + expression + "'");
      }
      if(next =='\"') {
         mark++;
         off++;
         
         while(off < source.length) {
            next = source[off++];
            
            if(next == '\"') {
               break;
            }
            if(next == '\\'){
               escape++;
            }
            length++;
         }
         if(next != '\"'){
            throw new IllegalStateException("Quotation not closed for '" + expression + "'");
         }
      } else {
         while(off < source.length) {
            next = source[off];
            
            if(terminal(next)) {
               break;
            }
            if(next == '\\'){
               escape++;
            }
            length++;
            off++;
         }
      }
      if(escape > 0) {
         return decode(mark, length);
      }
      return convert(mark, length);
          
   }
   
   private String convert(int off, int length) {
      if(length == 4) {         
         for(int i = 0; i < length; i++) {
            char next = source[off + i];
            
            if(next != NULL[i]) {
               return new String(source, off, length);
            }                       
         }
         return null;
      }
      return new String(source, off, length);
   }

   private String decode(int off, int length) {
      int limit = length + off;
      int write = off;
      int read = off;

      while(read < limit) {
         char current = source[read];
         
         if(current == '\\' && read + 1 < limit) {
            char next = source[read + 1];
            
            if(next == 'u' || next == 'U') {         
               source[write++] = escape(read + 2, 4);
               read += 6;
            } else {
               source[write++] = source[read++];
            }
         } else {
            source[write++] = source[read++];
         }
      }
      return new String(source, off, write - off);
   }
   
   private char escape(int off, int length) {
      int value = 0;
      
      for(int i = 0; i < length; i++) {
         char next = source[off + i];
         
         value <<= 4;
         value |= convert(next);
      }
      return (char)value;
   }
   
   private int convert(char value) {
      if(value >= '0' && value <= '9') {
         return value - '0'; 
      }
      if(value >= 'a' && value <= 'f') {
         return 10 + (value - 'a'); 
      }
      if(value >= 'A' && value <= 'F') {
         return 10 + (value - 'A'); 
      }
      throw new IllegalArgumentException("Character '" + value + "' is not hexidecimal");
   }
   
   private boolean terminal(char value) {
      switch(value) {
      case ' ': case ',':
      case '(': case ')':
         return true;         
      }
      return false;
   }
}
