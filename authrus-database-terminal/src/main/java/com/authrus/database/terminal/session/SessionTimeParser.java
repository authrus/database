package com.authrus.database.terminal.session;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SessionTimeParser {
   
   // Fri Jun 12 2015 19:20:06 GMT+1000 (AUS Eastern Standard Time)
   private static final String DATE_FORMAT = "EEE MMM dd yyyy HH:mm:ss 'GMT'z";
   private static final String DATE_PATTERN = ".*\\d+:\\d+:\\d+\\s+GMT([\\+|\\-|\\d]+).*";
   
   private final DateFormat format;
   
   public SessionTimeParser() {
      this.format = new SimpleDateFormat(DATE_FORMAT); 
   }

   public synchronized SessionTime parseTime(String text) {
      TimeZone zone = TimeZone.getDefault();
      long time = System.currentTimeMillis();
      
      try {
         if(text != null) {
            Pattern pattern = Pattern.compile(DATE_PATTERN);
            Matcher matcher = pattern.matcher(text);
            
            if(matcher.matches()) {            
               String token = matcher.group(1);
               Integer hours = Integer.parseInt(token);
               Integer offset = Math.round((hours / 100.0f) * 60.0f * 60.0f * 1000.0f);
               String[] zones = TimeZone.getAvailableIDs(offset);
               Date date = format.parse(text);
               
               if(zones.length > 0) {
                  zone = TimeZone.getTimeZone(zones[0]);
                  time = date.getTime();
               } else {
                  zone = format.getTimeZone();
                  time = date.getTime();
               }
            }         
         }
      } catch(Exception e) {
         throw new IllegalStateException("Unable to parse " + text, e);
      }         
      return SessionTime.builder()
            .text(text)
            .zone(zone)
            .time(time)
            .build();            
   }
}
