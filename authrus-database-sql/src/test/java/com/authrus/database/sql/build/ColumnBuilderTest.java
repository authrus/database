package com.authrus.database.sql.build;

import junit.framework.TestCase;

import com.authrus.database.Column;
import com.authrus.database.data.DataConstraint;
import com.authrus.database.data.DataType;
import com.authrus.database.sql.parse.QueryToken;
import com.authrus.database.sql.parse.QueryTokenType;

public class ColumnBuilderTest extends TestCase {

   public void testColumnWithNoDefault() throws Exception {
      StringBuilder expression = new StringBuilder();
      ColumnBuilder builder = new ColumnBuilder(expression, "age", 1);
      
      builder.update(new QueryToken(QueryTokenType.INT, "int".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.NOT_NULL, "not null".toCharArray()));      
      
      Column column = builder.createColumn();
      
      assertNotNull(column);
      assertEquals(column.getIndex(), 1);
      assertEquals(column.getName(), "age");
      assertEquals(column.getDataConstraint(), DataConstraint.REQUIRED);            
      assertEquals(column.getDataType(), DataType.INT);
      assertEquals(column.getDefaultValue().getDefault(column, null), null);
      
      System.err.println(expression);
   }

   public void testColumnWithLiteralDefault() throws Exception {
      StringBuilder expression = new StringBuilder();
      ColumnBuilder builder = new ColumnBuilder(expression, "age", 1);
      
      builder.update(new QueryToken(QueryTokenType.INT, "int".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.NOT_NULL, "not null".toCharArray()));      
      builder.update(new QueryToken(QueryTokenType.DEFAULT, "default".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.EXPRESSION, "18".toCharArray()));
      
      Column column = builder.createColumn();
      
      assertNotNull(column);
      assertEquals(column.getIndex(), 1);
      assertEquals(column.getName(), "age");
      assertEquals(column.getDataConstraint(), DataConstraint.REQUIRED);            
      assertEquals(column.getDataType(), DataType.INT);
      assertEquals(column.getDefaultValue().getDefault(column, null), 18);
      
      System.err.println(expression);
   }

   public void testColumnWithNoSequenceDefault() throws Exception {
      StringBuilder expression = new StringBuilder();
      ColumnBuilder builder = new ColumnBuilder(expression, "age", 1);
      
      builder.update(new QueryToken(QueryTokenType.INT, "int".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.NOT_NULL, "not null".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.DEFAULT, "default".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.EXPRESSION, "sequence".toCharArray()));  
      
      Column column = builder.createColumn();
      
      assertNotNull(column);
      assertEquals(column.getIndex(), 1);
      assertEquals(column.getName(), "age");
      assertEquals(column.getDataConstraint(), DataConstraint.REQUIRED);            
      assertEquals(column.getDataType(), DataType.INT);
      assertEquals(column.getDefaultValue().getDefault(column, null), 0);
      
      System.err.println(expression);
   }
}
