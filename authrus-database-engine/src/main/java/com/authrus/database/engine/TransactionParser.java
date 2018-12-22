package com.authrus.database.engine;

import com.authrus.database.common.parse.Parser;

public class TransactionParser extends Parser {

   private final Token origin;
   private final Token sequence;
   private final Token time;
   private final Token name;

   public TransactionParser() {
      this.sequence = new Token();
      this.origin = new Token();
      this.name = new Token();
      this.time = new Token();
   }  
   
   public TransactionParser(String text) {
      this();
      parse(text);
   }

   public String getOrigin() {
      return origin.toString();
   }
   
   public String getName() {
      return name.toString();
   }

   public Long getSequence() {
      return sequence.toLong();
   }

   public Long getTime() {
      return time.toLong();
   }   
   
   @Override
   protected void init() {
      sequence.clear();
      time.clear();
      origin.clear();
   }

   @Override
   protected void parse() {
      name();
      
      if(skip("@")) {
         origin();
         time();
         sequence();
      }
   }

   private void name() {
      name.off = off;

      while(off < count) {
         if(source[off] == '@') {
            break;
         }
         name.len++;  
         off++;
      }
   }   
   
   private void origin() {
      origin.off = off;

      while(off < count) {
         if(source[off++] == '.') {            
            break;
         }
         origin.len++;         
      }
   }
   
   private void time() {
      time.off = off;

      while(off < count) {
         if(source[off++] == '.') {            
            break;
         }
         time.len++;         
      }
   }
   
   private void sequence() {
      sequence.off = off;

      while(off < count) {
         if(source[off++] == '.') {            
            break;
         }
         sequence.len++;         
      }
   }

   private class Token {

      public String cache;
      public int off;
      public int len;

      public void clear() {
         cache = null;
         len = 0;
      }
      
      public Long toLong() {
         String value = toString();
         
         if(value != null) {
            return Long.parseLong(value);
         }
         return 0L;
      }
      
      @Override
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
