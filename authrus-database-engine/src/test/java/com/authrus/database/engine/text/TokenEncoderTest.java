package com.authrus.database.engine.text;

import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Calendar;
import java.util.GregorianCalendar;

import junit.framework.TestCase;

public class TokenEncoderTest extends TestCase {
   
   public void testEncoder() throws Exception {
      Calendar calendar = new GregorianCalendar();
      TokenEncoder encoder = new TokenEncoder(calendar);
      Writer writer = new OutputStreamWriter(System.err);
      
      encoder.encodeToken(writer, "Hello\r\nDolly");
      encoder.encodeToken(writer, "\"Hello Dolly\"");
      encoder.encodeToken(writer, "(Hello Dolly)");
   }

}
