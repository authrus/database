package com.authrus.database.sql;

import junit.framework.TestCase;

import com.authrus.database.sql.WhereClause;
import com.authrus.database.sql.build.WhereClauseBuilder;
import com.authrus.database.sql.parse.QueryToken;
import com.authrus.database.sql.parse.QueryTokenType;

public class WhereClauseBuilderTest extends TestCase {
   
   public void testBuilder() throws Exception {
      WhereClauseBuilder builder = new WhereClauseBuilder();     
      
      builder.update(new QueryToken(QueryTokenType.EXPRESSION, "a==b".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.AND, "and".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.EXPRESSION, "x==:name".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.OR, "or".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.EXPRESSION, "t>11.0f".toCharArray()));
      
      WhereClause clause = builder.createClause();
      
      assertEquals(clause.getConditions().get(0).getParameter().getColumn(), "a");
      assertEquals(clause.getConditions().get(0).getParameter().getName(), null);
      assertEquals(clause.getConditions().size(), 3);
      assertEquals(clause.getClause(), "a==? and x==? or t>?");
      
   }

}
