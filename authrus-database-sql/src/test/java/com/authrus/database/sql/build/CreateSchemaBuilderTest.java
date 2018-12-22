package com.authrus.database.sql.build;

import junit.framework.TestCase;

import com.authrus.database.Schema;
import com.authrus.database.data.DataType;
import com.authrus.database.sql.parse.QueryToken;
import com.authrus.database.sql.parse.QueryTokenType;

public class CreateSchemaBuilderTest extends TestCase {
   
   public void testCreateSchemaWithKey() throws Exception {
      CreateSchemaBuilder builder = new CreateSchemaBuilder();
      
      builder.update(new QueryToken(QueryTokenType.OPEN, "(".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.EXPRESSION, "x".toCharArray()));      
      builder.update(new QueryToken(QueryTokenType.INT, "int".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.NOT_NULL, "not null".toCharArray()));       
      builder.update(new QueryToken(QueryTokenType.COMMA, ",".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.EXPRESSION, "y".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.INT, "int".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.NOT_NULL, "not null".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.COMMA, ",".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.PRIMARY_KEY, "primary key".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.OPEN, "(".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.EXPRESSION, "x".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.CLOSE, ")".toCharArray()));       
      builder.update(new QueryToken(QueryTokenType.CLOSE, ")".toCharArray()));
      
      System.err.println(builder);
      
      Schema schema = builder.schema();
      
      assertNotNull(schema);
      assertEquals(schema.getCount(), 2);
      assertEquals(schema.getColumns().get(0), "x");
      assertEquals(schema.getColumns().get(1), "y");
   }
   
   public void testCreateSchemaWithCompositeKey() throws Exception {
      CreateSchemaBuilder builder = new CreateSchemaBuilder();
      
      builder.update(new QueryToken(QueryTokenType.OPEN, "(".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.EXPRESSION, "x".toCharArray()));      
      builder.update(new QueryToken(QueryTokenType.INT, "int".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.NOT_NULL, "not null".toCharArray()));       
      builder.update(new QueryToken(QueryTokenType.COMMA, ",".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.EXPRESSION, "y".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.INT, "int".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.NOT_NULL, "not null".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.COMMA, ",".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.EXPRESSION, "z".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.LONG, "long".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.NOT_NULL, "default sequence".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.COMMA, ",".toCharArray()));      
      builder.update(new QueryToken(QueryTokenType.PRIMARY_KEY, "primary key".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.OPEN, "(".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.EXPRESSION, "x".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.COMMA, ",".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.EXPRESSION, "y".toCharArray()));
      builder.update(new QueryToken(QueryTokenType.CLOSE, ")".toCharArray()));       
      builder.update(new QueryToken(QueryTokenType.CLOSE, ")".toCharArray()));
      
      System.err.println(builder);
      
      Schema schema = builder.schema();
      
      assertNotNull(schema);
      assertEquals(schema.getCount(), 3);
      assertEquals(schema.getColumns().get(0), "x");
      assertEquals(schema.getColumns().get(1), "y");
      assertEquals(schema.getColumns().get(2), "z");
      assertEquals(schema.getColumn(0).getDataType(), DataType.INT);
      assertEquals(schema.getColumn(1).getDataType(), DataType.INT);
      assertEquals(schema.getColumn(2).getDataType(), DataType.LONG); 
      assertEquals(schema.getKey().getColumns().get(0), "x");
      assertEquals(schema.getKey().getColumns().get(1), "y");      
   }
}
