package com.authrus.database.terminal;

import com.authrus.database.terminal.command.QueryRequestBuilder;

import junit.framework.TestCase;

public class QueryRequestBuilderTest extends TestCase {
   
   public void testBuilder() throws Exception {
      QueryRequestBuilder builder = new QueryRequestBuilder();
      
      assertEquals(builder.createRequest("select count(*) from test").getRepeat(), 1);
      assertEquals(builder.createRequest("  select count(*) from   test ").getRepeat(), 1);
      assertEquals(builder.createRequest(" repeat 5\r\n\r\n select count(*) from\r\n   test ").getRepeat(), 5);
      assertEquals(builder.createRequest(" repeat 5\r\n\r\n select count(*) from\r\n   test ").getQuery().getSource(),  "select count(*) from test");
      assertEquals(builder.createRequest(" repeat 5\r\n\r\n select count(*) from\r\n   test ").getQuery().getTable(),  "test");        
      assertEquals(builder.createRequest("REPEAT 15\r\n\r\n select count(*) from   test ").getRepeat(), 15);
      assertEquals(builder.createRequest("REPEAT 15\r\n\r\n select count(*) from   test ").getQuery().getSource(),  "select count(*) from test");
      assertEquals(builder.createRequest("REPEAT 15\r\n\r\n select count(*) from   test ").getQuery().getTable(),  "test");        
   }

}
