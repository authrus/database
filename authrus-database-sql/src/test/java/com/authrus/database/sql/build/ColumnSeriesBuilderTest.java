package com.authrus.database.sql.build;

import junit.framework.TestCase;

import com.authrus.database.ColumnSeries;
import com.authrus.database.sql.parse.QueryToken;
import com.authrus.database.sql.parse.QueryTokenType;

public class ColumnSeriesBuilderTest extends TestCase {

   public void testColumnSeries() throws Exception {
      StringBuilder expression = new StringBuilder();
      ColumnSeries series = new ColumnSeries();
      ColumnSeriesBuilder builder = new ColumnSeriesBuilder(expression, series);

      builder.update(new QueryToken(QueryTokenType.EXPRESSION, "a".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.INT, "int".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.NOT_NULL, "not null".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.COMMA, ",".toCharArray()));      
      builder.update(new QueryToken(QueryTokenType.EXPRESSION, "b".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.DOUBLE, "double".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.NOT_NULL, "not null".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.DEFAULT, "default".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.EXPRESSION, "1.2".toCharArray()));
      
      System.err.println(builder);
      
      ColumnSeries columns = builder.createSeries();
      
      assertNotNull(columns);
   }


}
