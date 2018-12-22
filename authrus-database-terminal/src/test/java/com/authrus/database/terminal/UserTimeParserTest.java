package com.authrus.database.terminal;

import com.authrus.database.terminal.session.SessionTime;
import com.authrus.database.terminal.session.SessionTimeParser;

import junit.framework.TestCase;

public class UserTimeParserTest extends TestCase {
   
   public void testUserTime() throws Exception {
      SessionTimeParser parser = new SessionTimeParser();
      SessionTime time = parser.parseTime("Fri Jun 12 2015 19:20:06 GMT+1000 (AUS Eastern Standard Time)");
      
      assertEquals(time.getZone().getRawOffset(), 36000000);
      assertEquals(time.getText(), "Fri Jun 12 2015 19:20:06 GMT+1000 (AUS Eastern Standard Time)");
   }

}
