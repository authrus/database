package com.authrus.database.engine.text;

import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

import junit.framework.TestCase;

public class TokenReaderTest extends TestCase {
   
   public void testReader() throws Exception {
      List<String> lines = Arrays.asList(
            "hello", 
            "bye", 
            "ok", 
            "\\u0070\\u0075\\u0062\\x\\u006c\\u0069\\u0063", 
            "\\u0063\\u006c\\u0061\\u0073\\u0073\\u0020\\u0054\\u0065\\u0073\\u0074",
            "\\u0070\\u0075\\u0062\\u006c\\u0069\\u0063-transport",
            "\\u005c\\u002c\\u0040\\u003d",
            "Hello\\u000d\\u000aDolly"
            );

      
      Calendar calendar = new GregorianCalendar();

      assertEquals(new TokenReader(calendar, " hello world!").readToken(), "hello");
      assertEquals(new TokenReader(calendar, "hello,world!").readToken(), "hello");
      assertEquals(new TokenReader(calendar, "\\u0070\\u0075\\u0062\\x\\u006c\\u0069\\u0063").readToken(), "pub\\xlic");
      assertEquals(new TokenReader(calendar, " \\u0063\\u006c\\u0061\\u0073\\u0073\\u0020\\u0054\\u0065\\u0073\\u0074 ").readToken(), "class Test");
      assertEquals(new TokenReader(calendar, " (a,b c) ").readTokens().size(), 3);
      assertEquals(new TokenReader(calendar, " (a,b c d) ").readTokens().size(), 4);
      assertEquals(new TokenReader(calendar, " (a,b c d) ").readTokens().get(0), "a");
      assertEquals(new TokenReader(calendar, " (a,b c d) ").readTokens().get(1), "b");
      assertEquals(new TokenReader(calendar, " (a,b c d) ").readTokens().get(2), "c");
      assertEquals(new TokenReader(calendar, " (a,b c d) ").readTokens().get(3), "d");
      assertEquals(new TokenReader(calendar, " (a,b c d)x ").readTokens().size(), 4);
      assertEquals(new TokenReader(calendar, " (a,b c d)x ").readTokens().get(0), "a");
      assertEquals(new TokenReader(calendar, " (a,b c d)x ").readTokens().get(1), "b");
      assertEquals(new TokenReader(calendar, " (a,b c d)x ").readTokens().get(2), "c");
      assertEquals(new TokenReader(calendar, " (a,b c d)x ").readTokens().get(3), "d");
      assertEquals(new TokenReader(calendar, " (a,\\u0063\\u006c\\u0061\\u0073\\u0073\\u0020\\u0054\\u0065\\u0073\\u0074 c d)x ").readTokens().size(), 4);
      assertEquals(new TokenReader(calendar, " (a,\\u0063\\u006c\\u0061\\u0073\\u0073\\u0020\\u0054\\u0065\\u0073\\u0074 c d)x ").readTokens().get(1), "class Test");
      assertEquals(new TokenReader(calendar, " (a) ").readTokens().get(0), "a");
      assertEquals(new TokenReader(calendar, " \\u0022HelloDolly\\u0022").readToken(), "\"HelloDolly\"");
      assertEquals(new TokenReader(calendar, " ( \"quoted string\", a, \"a\"  \"another str \")").readTokens().get(0), "quoted string");
      assertEquals(new TokenReader(calendar, " ( \"quoted string\", a, \"a\"  \"another str \")").readTokens().get(1), "a");
      assertEquals(new TokenReader(calendar, " ( \"quoted string\", a, \"a\"  \"another str \")").readTokens().get(2), "a");
      assertEquals(new TokenReader(calendar, " ( \"quoted string\", a, \"a\"  \"\\u0063\\u006c\\u0061\\u0073\\u0073\\u0020\\u0054\\u0065\\u0073\\u0074 \")").readTokens().get(3), "class Test ");
      assertNotNull(new TokenReader(calendar, "1997-18-11 12:30:01.235 2").readDate());
      assertEquals(new TokenReader(calendar, "23").readLong(), 23L);

   }
   

}
