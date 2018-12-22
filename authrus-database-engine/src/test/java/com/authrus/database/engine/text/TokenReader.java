package com.authrus.database.engine.text;

import static java.util.Calendar.DAY_OF_MONTH;
import static java.util.Calendar.HOUR_OF_DAY;
import static java.util.Calendar.MILLISECOND;
import static java.util.Calendar.MINUTE;
import static java.util.Calendar.MONTH;
import static java.util.Calendar.SECOND;
import static java.util.Calendar.YEAR;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class TokenReader  {  
   
   private TokenDecoder decoder;
   private Calendar calendar;
   
   public TokenReader(Calendar calendar, String expression) {
      this.decoder = new TokenDecoder(expression);     
      this.calendar = calendar;
   }   
   
   public boolean isFinished() {
      int count = decoder.count();
      
      while(count > 0) {
         char next = decoder.peek();
      
         if(next != ')' && next != ' ') {
            return false;
         }
         next = decoder.next();
         count = decoder.count();
      }
      return true;
   }
   
   public char readChar() {
      int count = decoder.count();
      
      if(count <= 0) {
         throw new IllegalStateException("Token reader has no more data");
      }
      return decoder.next();
   }
   
   public long readLong() {
      int count = decoder.count();
      
      if(count <= 0) {
         throw new IllegalStateException("Token reader has no more data");
      }
      Number number = decoder.number();
      
      if(number == null) {
         throw new IllegalStateException("Token was not a number");
      }
      return number.longValue();
   }
   
   public int readInt() {
      int count = decoder.count();
      
      if(count <= 0) {
         throw new IllegalStateException("Token reader has no more data");
      }
      Number number = decoder.number();
      
      if(number == null) {
         throw new IllegalStateException("Token was not a number");
      }
      return number.intValue();
   }   
   
   public Date readDate() {
      int count = decoder.count();
      
      if(count < 23) {
         throw new IllegalStateException("Insufficient data to read date");
      }
      char[] tokens = {'-', '-', ' ', ':', ':', '.', ' '}; // must be followed by space!?
      int[] parts = {0, 0, 0, 0, 0, 0, 0};
      
      for(int i = 0; i < tokens.length; i++) {
         int number = readInt();
         
         if(!decoder.skip(tokens[i])) {
            throw new IllegalStateException("Date does not match yyyy-MM-dd HH:mm:ss.SSS");
         }
         parts[i] = number;
      }             
      calendar.set(YEAR, parts[0]);
      calendar.set(MONTH, parts[1] - 1);
      calendar.set(DAY_OF_MONTH, parts[2]);
      calendar.set(HOUR_OF_DAY, parts[3]);
      calendar.set(MINUTE, parts[4]);
      calendar.set(SECOND, parts[5]);
      calendar.set(MILLISECOND, parts[6]);
      
      return calendar.getTime();      
   }
   
   public String readToken() {
      int count = decoder.count();
      
      if(count <= 0) {
         throw new IllegalStateException("Token reader has no more data");
      }
      while(count > 0) {
         char next = decoder.peek();
      
         if(next != ')' && next != '(' && next != ',' && next != ' ') {
            break;
         }
         next = decoder.next();
         count = decoder.count();
      }
      return decoder.text();
   }
   
   public List<String> readTokens() {
      int count = decoder.count();
      
      if(count <= 0) {
         throw new IllegalStateException("Token reader has no more data");
      }
      while(count > 0) {
         char next = decoder.peek();
      
         if(next != ')' && next != ',' && next != ' ') {
            break;
         }
         next = decoder.next();
         count = decoder.count();
      }
      char brace = decoder.peek();
      
      if(brace != '(') {
         throw new IllegalStateException("Token group could not be found");
      }
      List<String> tokens = new ArrayList<String>();
      
      while(count > 0 && brace != ')') {
         char next = decoder.peek();
         
         if(next == ')') {
            decoder.next();
            break;
         }
         if(next != '(' && next != ',' && next != ' ') {
            String token = decoder.text();            
            tokens.add(token);            
         }         
         brace = decoder.next();
         count = decoder.count();
      }
      return tokens;
   }
 
}
