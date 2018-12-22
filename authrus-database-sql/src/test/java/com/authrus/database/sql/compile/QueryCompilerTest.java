package com.authrus.database.sql.compile;

import junit.framework.TestCase;

import com.authrus.database.sql.ParameterType;
import com.authrus.database.sql.Query;
import com.authrus.database.sql.QueryConverter;
import com.authrus.database.sql.Verb;
import com.authrus.database.sql.build.IdentityConverter;
import com.authrus.database.sql.build.QueryProcessor;

public class QueryCompilerTest extends TestCase {

   public void testLike() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("select a,b,c from test where a like :x");

      assertEquals(builder.getVerb(), Verb.SELECT);
      assertEquals(builder.getTable(), "test");
      assertEquals(builder.getColumns().size(), 3);
      assertEquals(builder.getColumns().get(0), "a");
      assertEquals(builder.getColumns().get(1), "b");
      assertEquals(builder.getColumns().get(2), "c");
      assertEquals(builder.getWhereClause().getConditions().size(), 1);
      assertEquals(builder.getWhereClause().getConditions().get(0).getComparison(), "like");
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getColumn(), "a");
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getName(), "x");
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getValue(), null);

      QueryCompiler compiler = new QueryCompiler();
      String result = compiler.compile(builder, new Object[] { "text%" });

      System.err.println(result);
      assertEquals("select a, b, c from test where a like 'text%'", result);
   }
   
   public void testApproxLike() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("select a,b,c from test where a =~ :x");

      assertEquals(builder.getVerb(), Verb.SELECT);
      assertEquals(builder.getTable(), "test");
      assertEquals(builder.getColumns().size(), 3);
      assertEquals(builder.getColumns().get(0), "a");
      assertEquals(builder.getColumns().get(1), "b");
      assertEquals(builder.getColumns().get(2), "c");
      assertEquals(builder.getWhereClause().getConditions().size(), 1);
      assertEquals(builder.getWhereClause().getConditions().get(0).getComparison(), "=~");
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getColumn(), "a");
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getName(), "x");
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getValue(), null);

      QueryCompiler compiler = new QueryCompiler();
      String result = compiler.compile(builder, new Object[] { "text%" });

      System.err.println(result);
      assertEquals("select a, b, c from test where a =~ 'text%'", result);
   }
   

   public void testUpdateStatement() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("update duh set x = :x, y = :somVar, z = 10, a = :otherVar where b = :lastVar");

      assertEquals(builder.getVerb(), Verb.UPDATE);
      assertEquals(builder.getTable(), "duh");
      assertEquals(builder.getColumns().size(), 0);
      assertEquals(builder.getParameters().size(), 4);
      assertEquals(builder.getParameters().get(0).getColumn(), "x");
      assertEquals(builder.getParameters().get(0).getName(), "x");
      assertEquals(builder.getParameters().get(1).getColumn(), "y");
      assertEquals(builder.getParameters().get(1).getName(), "somVar");
      assertEquals(builder.getParameters().get(2).getColumn(), "z");
      assertEquals(builder.getParameters().get(2).getName(), null);
      assertEquals(builder.getParameters().get(3).getColumn(), "a");
      assertEquals(builder.getParameters().get(3).getName(), "otherVar");
      assertEquals(builder.getWhereClause().getConditions().size(), 1);
      assertEquals(builder.getWhereClause().getConditions().get(0).getComparison(), "=");
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getColumn(), "b");
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getName(), "lastVar");
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getValue(), null);

      QueryCompiler compiler = new QueryCompiler();
      String result = compiler.compile(builder, new Object[] { "x", "somVar", "otherVar", "lastVar" });

      System.err.println(result);
      assertEquals("update duh set x = 'x', y = 'somVar', z = '10', a = 'otherVar' where b = 'lastVar'", result);
   }

   public void testSelectStatement() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("select a, b, c from alphabet_v1 where x = :x and y = :x");

      assertEquals(builder.getVerb(), Verb.SELECT);
      assertEquals(builder.getTable(), "alphabet_v1");
      assertEquals(builder.getColumns().size(), 3);
      assertEquals(builder.getColumns().get(0), "a");
      assertEquals(builder.getColumns().get(1), "b");
      assertEquals(builder.getColumns().get(2), "c");
      assertEquals(builder.getWhereClause().getConditions().size(), 2);
      assertEquals(builder.getWhereClause().getConditions().get(0).getComparison(), "=");
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getColumn(), "x");
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getName(), "x");
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getValue(), null);
      assertEquals(builder.getWhereClause().getConditions().get(1).getComparison(), "=");
      assertEquals(builder.getWhereClause().getConditions().get(1).getParameter().getColumn(), "y");
      assertEquals(builder.getWhereClause().getConditions().get(1).getParameter().getName(), "x");
      assertEquals(builder.getWhereClause().getConditions().get(1).getParameter().getValue(), null);

      QueryCompiler compiler = new QueryCompiler();
      String result = compiler.compile(builder, new Object[] { 22.d, 11.d });

      System.err.println(result);
      assertEquals("select a, b, c from alphabet_v1 where x = 22.0 and y = 11.0", result);
   }

   public void testDeleteStatement() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("delete from blah where x = :y");

      assertEquals(builder.getVerb(), Verb.DELETE);
      assertEquals(builder.getTable(), "blah");
      assertEquals(builder.getColumns().size(), 0);
      assertEquals(builder.getParameters().size(), 0);
      assertEquals(builder.getWhereClause().getConditions().size(), 1);
      assertEquals(builder.getWhereClause().getConditions().get(0).getComparison(), "=");
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getColumn(), "x");
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getName(), "y");
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getValue(), null);

      QueryCompiler compiler = new QueryCompiler();
      String result = compiler.compile(builder, new Object[] { 22.d });

      System.err.println(result);
      assertEquals("delete from blah where x = 22.0", result);
   }

   public void testInsertStatement() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("insert into example (name, address, age) values (:name, :address, :age)");

      assertEquals(builder.getVerb(), Verb.INSERT);
      assertEquals(builder.getTable(), "example");
      assertEquals(builder.getColumns().size(), 3);
      assertEquals(builder.getParameters().size(), 3);
      assertEquals(builder.getParameters().get(0).getName(), "name");
      assertEquals(builder.getParameters().get(1).getName(), "address");
      assertEquals(builder.getParameters().get(2).getName(), "age");
      assertEquals(builder.getParameters().get(0).getValue(), null);
      assertEquals(builder.getParameters().get(1).getValue(), null);
      assertEquals(builder.getParameters().get(2).getValue(), null);
      assertEquals(builder.getParameters().get(0).getColumn(), null);
      assertEquals(builder.getParameters().get(1).getColumn(), null);
      assertEquals(builder.getParameters().get(2).getColumn(), null);
      assertEquals(builder.getParameters().get(0).getType(), ParameterType.NAME);
      assertEquals(builder.getParameters().get(1).getType(), ParameterType.NAME);
      assertEquals(builder.getParameters().get(2).getType(), ParameterType.NAME);

      QueryCompiler compiler = new QueryCompiler();
      String result = compiler.compile(builder, new Object[] { "Tom", "1 Some Street", 11 });

      System.err.println(result);
      assertEquals("insert into example (name, address, age) values ('Tom', '1 Some Street', 11)", result);
   }
}
