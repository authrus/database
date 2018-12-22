package com.authrus.database.engine.text;

import junit.framework.TestCase;

public class TokenDecoderTest extends TestCase {
   
   public void testDecoder() throws Exception {
      TokenDecoder decoder = new TokenDecoder("this is a \"simple\" test 12");
      
      assertEquals(decoder.text(), "this");
      assertEquals(decoder.peek(), ' ');
      assertEquals(decoder.next(), ' ');
      assertEquals(decoder.text(), "is");
      assertEquals(decoder.next(), ' ');
      assertEquals(decoder.text(), "a");
      assertEquals(decoder.next(), ' ');
      assertEquals(decoder.text(), "simple");
      assertEquals(decoder.next(), ' ');
      assertEquals(decoder.text(), "test");
      assertEquals(decoder.next(), ' ');
      assertEquals(decoder.number().intValue(), 12);        
   }

   public void testEscapeDecoder() throws Exception {
      TokenDecoder decoder = new TokenDecoder("a \\u0063\\u006c\\u0061\\u0073\\u0073\\u0020\\u0054\\u0065\\u0073\\u0074 714 ");
      
      assertEquals(decoder.text(), "a");
      assertEquals(decoder.peek(), ' ');
      assertEquals(decoder.next(), ' ');
      assertEquals(decoder.text(), "class Test");   
      assertEquals(decoder.next(), ' ');
      assertEquals(decoder.count(), 4);  
      assertEquals(decoder.number().intValue(), 714);
      assertEquals(decoder.count(), 1);  
      assertEquals(decoder.peek(), ' ');
   }
   
}
