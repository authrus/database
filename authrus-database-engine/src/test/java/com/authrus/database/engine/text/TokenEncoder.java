package com.authrus.database.engine.text;

import static java.util.Calendar.DAY_OF_MONTH;
import static java.util.Calendar.HOUR_OF_DAY;
import static java.util.Calendar.MILLISECOND;
import static java.util.Calendar.MINUTE;
import static java.util.Calendar.MONTH;
import static java.util.Calendar.SECOND;
import static java.util.Calendar.YEAR;

import java.io.IOException;
import java.io.Writer;
import java.util.Calendar;

public class TokenEncoder {

   private static final String[] SYMBOLS = {"\\u005c", "\\u002c", "\\u003d", "\\u000d", "\\u000a", "\\u0022", "\\u0028", "\\u0029"};   
   private static final String ZERO = "1970-01-01 10:00:00.000";
   private static final String SPECIALS = "\\,=\r\n\"()";
   private static final String NULL_VALUE = "NULL";
   
   private final Calendar calendar;
   
   public TokenEncoder(Calendar calendar){
      this.calendar = calendar;
   }

   public void encodeDate(Writer writer, long date) throws IOException {       
      calendar.setTimeInMillis(date);
      
      if(date > 0) {
         Integer[] parts = new Integer[7];
         
         parts[0] = calendar.get(YEAR);
         parts[1] = calendar.get(MONTH);
         parts[2] = calendar.get(DAY_OF_MONTH);
         parts[3] = calendar.get(HOUR_OF_DAY);
         parts[4] = calendar.get(MINUTE);
         parts[5] = calendar.get(SECOND);
         parts[6] = calendar.get(MILLISECOND);
         
         encodeInteger(writer, parts[0], 4);
         writer.write("-");
         encodeInteger(writer, parts[1] + 1, 2);
         writer.write("-");
         encodeInteger(writer, parts[2], 2);
         writer.write(" ");
         encodeInteger(writer, parts[3], 2);
         writer.write(":");
         encodeInteger(writer, parts[4], 2);
         writer.write(":");
         encodeInteger(writer, parts[5], 2);
         writer.write(".");
         encodeInteger(writer, parts[6], 3);
      } else {
         writer.write(ZERO);              
      }
   }
   
   public void encodeToken(Writer writer, String token) throws IOException {
      if(token != null) {
         int space = token.indexOf(' ');
         
         if(space != -1) {
            writer.write('"');
         }
         int length = token.length();
         
         for(int i = 0; i < length; i++) {
            char next = token.charAt(i);
            int index = SPECIALS.indexOf(next);
            
            if(index == -1) {
               writer.write(next);
            } else {            
               writer.write(SYMBOLS[index]);
            }
         }
         if(space != -1) {
            writer.write('"');
         }      
      } else {
         writer.write(NULL_VALUE);
      }
   }
   
   private void encodeInteger(Writer writer, int value, int width) throws IOException {
      String token = String.valueOf(value);
      int length = token.length();
      int fill = width - length;
      
      for(int i = 0; i < fill; i++) {
         writer.write('0');
      }
      writer.write(token);
   }
}
