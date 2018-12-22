package com.authrus.database.sql.build;

import junit.framework.TestCase;

import com.authrus.database.function.DefaultFunction;
import com.authrus.database.function.DefaultValue;
import com.authrus.database.sql.parse.QueryToken;
import com.authrus.database.sql.parse.QueryTokenType;

public class DefaultValueBuilderTest extends TestCase {
   
   public void testBuilderFunction() throws Exception {
      StringBuilder expression = new StringBuilder();
      DefaultValueBuilder builder = new DefaultValueBuilder(expression);
      
      builder.update(new QueryToken(QueryTokenType.EXPRESSION, "sequence".toCharArray()));
      
      DefaultValue value = builder.createDefault();
      
      assertEquals(value.getExpression(), "sequence");
      assertEquals(value.getFunction(), DefaultFunction.SEQUENCE);
   }
   
   public void testBuilderLiteral() throws Exception {
      StringBuilder expression = new StringBuilder();
      DefaultValueBuilder builder = new DefaultValueBuilder(expression);
      
      builder.update(new QueryToken(QueryTokenType.EXPRESSION, "112".toCharArray()));
      
      DefaultValue value = builder.createDefault();
      
      assertEquals(value.getExpression(), "112");
      assertEquals(value.getFunction(), DefaultFunction.LITERAL);
   }
   
   
   public void testBuilderDoubleLiteral() throws Exception {
      StringBuilder expression = new StringBuilder();
      DefaultValueBuilder builder = new DefaultValueBuilder(expression);
      
      builder.update(new QueryToken(QueryTokenType.EXPRESSION, "112.2".toCharArray()));
      
      DefaultValue value = builder.createDefault();
      
      assertEquals(value.getExpression(), "112.2");
      assertEquals(value.getFunction(), DefaultFunction.LITERAL);
   }
}
