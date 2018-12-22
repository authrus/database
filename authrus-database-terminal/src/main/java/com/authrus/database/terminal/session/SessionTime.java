package com.authrus.database.terminal.session;

import java.util.TimeZone;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@AllArgsConstructor
public class SessionTime {

   private final TimeZone zone;
   private final String text;
   private final long time;   
   
   public TimeZone getZone() {
      if(zone == null) {
         return TimeZone.getDefault();
      }
      return zone;
   }
}

