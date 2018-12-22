package com.authrus.database.sql;

import junit.framework.TestCase;

import com.authrus.database.data.DataConstraint;
import com.authrus.database.data.DataType;
import com.authrus.database.function.DefaultFunction;
import com.authrus.database.sql.Query;
import com.authrus.database.sql.QueryConverter;
import com.authrus.database.sql.Verb;
import com.authrus.database.sql.build.IdentityConverter;
import com.authrus.database.sql.build.QueryProcessor;

public class QueryProcessorTest extends TestCase {
   
   public void testCreateTable() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("create table t(x symbol not null default 'blah', y text not null, z int, primary key(x, y))");
      
      assertEquals(builder.getVerb(), Verb.CREATE_TABLE);
      assertEquals(builder.getTable(), "t"); 
      assertEquals(builder.getCreateSchema().getCount(), 3);        
      assertEquals(builder.getCreateSchema().getColumn("x").getDataType(), DataType.SYMBOL);
      assertEquals(builder.getCreateSchema().getColumn("x").getDataConstraint(), DataConstraint.REQUIRED);
      assertEquals(builder.getCreateSchema().getColumn("x").getDefaultValue().getExpression(), "'blah'");
      assertEquals(builder.getCreateSchema().getColumn("x").getDefaultValue().getFunction(), DefaultFunction.LITERAL);         
      assertEquals(builder.getCreateSchema().getColumn("y").getDataType(), DataType.TEXT);
      assertEquals(builder.getCreateSchema().getColumn("y").getDataConstraint(), DataConstraint.REQUIRED);            
      assertEquals(builder.getCreateSchema().getColumn("z").getDataType(), DataType.INT);      
      assertEquals(builder.getCreateSchema().getColumn("z").getDataConstraint(), DataConstraint.OPTIONAL);
      assertEquals(builder.getCreateSchema().getKey().getCount(), 2);      
      assertEquals(builder.getCreateSchema().getKey().getColumn("x").getDataType(), DataType.SYMBOL);
      assertEquals(builder.getCreateSchema().getKey().getColumn("y").getDataType(), DataType.TEXT);        
   } 
   
   public void testBeginTransaction() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("begin transaction x on table");
      
      assertEquals(builder.getVerb(), Verb.BEGIN);
      assertEquals(builder.getName(), "x");
      assertEquals(builder.getTable(), "table");
   }
   
   public void testBeginTransactionNoName() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("begin transaction on table");
      
      assertEquals(builder.getVerb(), Verb.BEGIN);
      assertEquals(builder.getName(), null);
      assertEquals(builder.getTable(), "table");
   }
   
   public void testBeginNoName() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("begin on table");
      
      assertEquals(builder.getVerb(), Verb.BEGIN);
      assertEquals(builder.getName(), null);
      assertEquals(builder.getTable(), "table");
   }
   
   public void testBegin() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("begin");
      
      assertEquals(builder.getVerb(), Verb.BEGIN);
      assertEquals(builder.getName(), null);
      assertEquals(builder.getTable(), null);
   }
   
   public void testCommit() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("commit table");
      
      assertEquals(builder.getVerb(), Verb.COMMIT);
      assertEquals(builder.getTable(), "table");
      assertEquals(builder.getName(), null);
   }  
   
   public void testCommitNoName() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("commit");
      
      assertEquals(builder.getVerb(), Verb.COMMIT);
      assertEquals(builder.getName(), null);
      assertEquals(builder.getTable(), null);
   }   
   
   public void testInsertIntoSelect() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("insert into x (a, b, c, d) select a, b, c, d from blah where a like '%6'");
      
      assertEquals(builder.getVerb(), Verb.INSERT);
      assertEquals(builder.getTable(), "x");
      assertEquals(builder.getTables().size(), 2);
      assertEquals(builder.getTables().get(0), "x"); 
      assertEquals(builder.getTables().get(1), "blah");       
      assertEquals(builder.getColumns().size(), 4);
      assertEquals(builder.getColumns().get(0), "a"); 
      assertEquals(builder.getColumns().get(1), "b"); 
      assertEquals(builder.getColumns().get(2), "c"); 
      assertEquals(builder.getColumns().get(3), "d");
      assertEquals(builder.getParameters().size(), 4);
      assertEquals(builder.getParameters().get(0).getValue(), "a");
      assertEquals(builder.getParameters().get(1).getValue(), "b");
      assertEquals(builder.getParameters().get(2).getValue(), "c");
      assertEquals(builder.getParameters().get(3).getValue(), "d");   
      assertEquals(builder.getWhereClause().getConditions().size(), 1);
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getColumn(), "a"); 
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getName(), null); 
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getType(), ParameterType.VALUE);      
   } 
   
   public void testInsertIntoSelectWithBraces() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("insert into x (a, b, c, d) select ( a, b, c,d) from blah where a like '%6'");
      
      assertEquals(builder.getVerb(), Verb.INSERT);
      assertEquals(builder.getTable(), "x");
      assertEquals(builder.getTables().size(), 2);
      assertEquals(builder.getTables().get(0), "x"); 
      assertEquals(builder.getTables().get(1), "blah");       
      assertEquals(builder.getColumns().size(), 4);
      assertEquals(builder.getColumns().get(0), "a"); 
      assertEquals(builder.getColumns().get(1), "b"); 
      assertEquals(builder.getColumns().get(2), "c"); 
      assertEquals(builder.getColumns().get(3), "d");
      assertEquals(builder.getParameters().size(), 4);
      assertEquals(builder.getParameters().get(0).getValue(), "a");
      assertEquals(builder.getParameters().get(1).getValue(), "b");
      assertEquals(builder.getParameters().get(2).getValue(), "c");
      assertEquals(builder.getParameters().get(3).getValue(), "d");   
      assertEquals(builder.getWhereClause().getConditions().size(), 1);
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getColumn(), "a"); 
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getName(), null); 
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getType(), ParameterType.VALUE);      
   }    
   
   public void testLiteralAndQuoteParameters() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("insert into example (name, address, age) values ('tom', '11 some street', 22)");
      
      assertEquals(builder.getVerb(), Verb.INSERT);
      assertEquals(builder.getTable(), "example"); 
      assertEquals(builder.getColumns().size(), 3);     
      assertEquals(builder.getParameters().size(), 3);
      assertEquals(builder.getParameters().get(0).getValue(), "tom");
      assertEquals(builder.getParameters().get(1).getValue(), "11 some street");
      assertEquals(builder.getParameters().get(2).getValue(), "22");
      assertEquals(builder.getParameters().get(0).getName(), null);
      assertEquals(builder.getParameters().get(1).getName(), null); 
      assertEquals(builder.getParameters().get(2).getName(), null); 
      assertEquals(builder.getParameters().get(0).getColumn(), null);
      assertEquals(builder.getParameters().get(1).getColumn(), null);
      assertEquals(builder.getParameters().get(2).getColumn(), null);
      assertEquals(builder.getParameters().get(0).getType(), ParameterType.VALUE);
      assertEquals(builder.getParameters().get(1).getType(), ParameterType.VALUE);  
      assertEquals(builder.getParameters().get(2).getType(), ParameterType.VALUE);   
   }  
   
   public void testParametersWithConditions() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("update example set address = ? where name == ?");
      
      assertEquals(builder.getVerb(), Verb.UPDATE);
      assertEquals(builder.getTable(), "example"); 
      assertEquals(builder.getColumns().size(), 0);     
      assertEquals(builder.getParameters().size(), 1);
      assertEquals(builder.getParameters().get(0).getColumn(), "address");
      assertEquals(builder.getParameters().get(0).getName(), null); 
      assertEquals(builder.getParameters().get(0).getValue(), null);
      assertEquals(builder.getParameters().get(0).getType(), ParameterType.TOKEN);       
      assertEquals(builder.getOrderByClause().getDirection(), null);
      assertEquals(builder.getOrderByClause().getClause(), "");
      assertEquals(builder.getWhereClause().getConditions().size(), 1);
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getColumn(), "name");
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getValue(), null);
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getName(), null);
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getType(), ParameterType.TOKEN);     
      assertEquals(builder.getWhereClause().getClause(), "name==?");  
   }
   
   public void testNamedUpdate() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("update example set address = :address where name == :name");
      
      assertEquals(builder.getVerb(), Verb.UPDATE);
      assertEquals(builder.getTable(), "example"); 
      assertEquals(builder.getColumns().size(), 0);     
      assertEquals(builder.getParameters().size(), 1);
      assertEquals(builder.getOrderByClause().getDirection(), null);
      assertEquals(builder.getOrderByClause().getClause(), "");
      assertEquals(builder.getWhereClause().getConditions().size(), 1);
      assertEquals(builder.getWhereClause().getClause(), "name==?");  
   }
   
   public void testCountRecords() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("select count(*) from foo where a == b and c == d");
      
      assertEquals(builder.getVerb(), Verb.SELECT);
      assertEquals(builder.getTable(), "foo"); 
      assertEquals(builder.getColumns().size(), 1);     
      assertEquals(builder.getParameters().size(), 0);
      assertEquals(builder.getOrderByClause().getDirection(), null);
      assertEquals(builder.getOrderByClause().getClause(), "");
      assertEquals(builder.getWhereClause().getConditions().size(), 2);
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getColumn(), "a");
      assertEquals(builder.getWhereClause().getConditions().get(1).getParameter().getColumn(), "c");
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getValue(), "b");
      assertEquals(builder.getWhereClause().getConditions().get(1).getParameter().getValue(), "d");
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getName(), null);
      assertEquals(builder.getWhereClause().getConditions().get(1).getParameter().getName(), null);
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getType(), ParameterType.VALUE);
      assertEquals(builder.getWhereClause().getConditions().get(1).getParameter().getType(), ParameterType.VALUE);       
      assertEquals(builder.getWhereClause().getClause(), "a==? and c==?");
   }

   public void testUpdate() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("update blah set x=?, y=?, z=?");
      
      assertEquals(builder.getVerb(), Verb.UPDATE);
      assertEquals(builder.getTable(), "blah"); 
      assertEquals(builder.getColumns().size(), 0);     
      assertEquals(builder.getParameters().size(), 3);
      assertEquals(builder.getParameters().get(0).getValue(), null);
      assertEquals(builder.getParameters().get(1).getValue(), null);
      assertEquals(builder.getParameters().get(2).getValue(), null);
      assertEquals(builder.getParameters().get(0).getName(), null);
      assertEquals(builder.getParameters().get(1).getName(), null);
      assertEquals(builder.getParameters().get(2).getName(), null);      
      assertEquals(builder.getParameters().get(0).getColumn(), "x");
      assertEquals(builder.getParameters().get(1).getColumn(), "y");
      assertEquals(builder.getParameters().get(2).getColumn(), "z");
      assertEquals(builder.getParameters().get(0).getType(), ParameterType.TOKEN);
      assertEquals(builder.getParameters().get(1).getType(), ParameterType.TOKEN);
      assertEquals(builder.getParameters().get(2).getType(), ParameterType.TOKEN);
      assertEquals(builder.getOrderByClause().getDirection(), null);
      assertEquals(builder.getOrderByClause().getClause(), "");
      assertEquals(builder.getWhereClause().getConditions().size(), 0);
      assertEquals(builder.getWhereClause().getClause(), "");
   }
   
   public void testDeleteWithWhereClauseAndNamedParameters() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("delete from table where a == :foo  and b==:bar");
      
      assertEquals(builder.getVerb(), Verb.DELETE);
      assertEquals(builder.getTable(), "table"); 
      assertEquals(builder.getColumns().size(), 0);     
      assertEquals(builder.getParameters().size(), 0);
      assertEquals(builder.getOrderByClause().getDirection(), null);
      assertEquals(builder.getOrderByClause().getClause(), "");
      assertEquals(builder.getWhereClause().getConditions().size(), 2);
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getColumn(), "a");      
      assertEquals(builder.getWhereClause().getConditions().get(1).getParameter().getColumn(), "b");
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getName(), "foo");      
      assertEquals(builder.getWhereClause().getConditions().get(1).getParameter().getName(), "bar");
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getValue(), null);      
      assertEquals(builder.getWhereClause().getConditions().get(1).getParameter().getValue(), null);
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getType(), ParameterType.NAME);      
      assertEquals(builder.getWhereClause().getConditions().get(1).getParameter().getType(), ParameterType.NAME);
      assertEquals(builder.getWhereClause().getConditions().get(0).getComparison(), "==");      
      assertEquals(builder.getWhereClause().getConditions().get(1).getComparison(), "==");        
      assertEquals(builder.getWhereClause().getClause(), "a==? and b==?");
   }
   
   public void testDeleteWithWhereClause() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("delete from table where a == ?  and b == ?");
      
      assertEquals(builder.getVerb(), Verb.DELETE);
      assertEquals(builder.getTable(), "table"); 
      assertEquals(builder.getColumns().size(), 0);     
      assertEquals(builder.getParameters().size(), 0);
      assertEquals(builder.getOrderByClause().getDirection(), null);
      assertEquals(builder.getOrderByClause().getClause(), "");
      assertEquals(builder.getWhereClause().getConditions().size(), 2);
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getColumn(), "a");      
      assertEquals(builder.getWhereClause().getConditions().get(1).getParameter().getColumn(), "b");
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getName(), null);      
      assertEquals(builder.getWhereClause().getConditions().get(1).getParameter().getName(), null);
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getValue(), null);      
      assertEquals(builder.getWhereClause().getConditions().get(1).getParameter().getValue(), null);
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getType(), ParameterType.TOKEN);      
      assertEquals(builder.getWhereClause().getConditions().get(1).getParameter().getType(), ParameterType.TOKEN);
      assertEquals(builder.getWhereClause().getClause(), "a==? and b==?");
   }
   
   public void testDelete() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("delete from table");
      
      assertEquals(builder.getVerb(), Verb.DELETE);
      assertEquals(builder.getTable(), "table"); 
      assertEquals(builder.getColumns().size(), 0);     
      assertEquals(builder.getParameters().size(), 0);
      assertEquals(builder.getOrderByClause().getDirection(), null);
      assertEquals(builder.getOrderByClause().getClause(), "");
      assertEquals(builder.getWhereClause().getConditions().size(), 0);
      assertEquals(builder.getWhereClause().getClause(), "");
   }

   public void testInsertNamedParameters() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("insert into table (x, y, z) values (:name, :address, :age)");
      
      assertEquals(builder.getVerb(), Verb.INSERT);
      assertEquals(builder.getTable(), "table"); 
      assertEquals(builder.getColumns().size(), 3);
      assertEquals(builder.getColumns().get(0), "x");
      assertEquals(builder.getColumns().get(1), "y");
      assertEquals(builder.getColumns().get(2), "z");      
      assertEquals(builder.getParameters().size(), 3);
      assertEquals(builder.getParameters().get(0).getColumn(), null);
      assertEquals(builder.getParameters().get(1).getColumn(), null);
      assertEquals(builder.getParameters().get(2).getColumn(), null);      
      assertEquals(builder.getParameters().get(0).getName(), "name");
      assertEquals(builder.getParameters().get(1).getName(), "address");
      assertEquals(builder.getParameters().get(2).getName(), "age");
      assertEquals(builder.getParameters().get(0).getValue(), null);
      assertEquals(builder.getParameters().get(1).getValue(), null);
      assertEquals(builder.getParameters().get(2).getValue(), null);
      assertEquals(builder.getParameters().get(0).getType(), ParameterType.NAME);
      assertEquals(builder.getParameters().get(1).getType(), ParameterType.NAME);
      assertEquals(builder.getParameters().get(2).getType(), ParameterType.NAME);
      assertEquals(builder.getOrderByClause().getDirection(), null);
      assertEquals(builder.getOrderByClause().getClause(), "");
      assertEquals(builder.getWhereClause().getConditions().size(), 0);
      assertEquals(builder.getWhereClause().getClause(), "");
   }
   
   public void testInsertImplicitColumns() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("insert into table values (?, ?, ?, ?)");
      
      assertEquals(builder.getVerb(), Verb.INSERT);
      assertEquals(builder.getTable(), "table"); 
      assertEquals(builder.getColumns().size(), 0);     
      assertEquals(builder.getParameters().size(), 4);
      assertEquals(builder.getParameters().get(0).getColumn(), null);
      assertEquals(builder.getParameters().get(1).getColumn(), null);
      assertEquals(builder.getParameters().get(2).getColumn(), null); 
      assertEquals(builder.getParameters().get(3).getColumn(), null);      
      assertEquals(builder.getParameters().get(0).getValue(), null);
      assertEquals(builder.getParameters().get(1).getValue(), null);
      assertEquals(builder.getParameters().get(2).getValue(), null);
      assertEquals(builder.getParameters().get(3).getValue(), null);
      assertEquals(builder.getParameters().get(0).getName(), null);
      assertEquals(builder.getParameters().get(1).getName(), null);
      assertEquals(builder.getParameters().get(2).getName(), null);
      assertEquals(builder.getParameters().get(3).getName(), null);
      assertEquals(builder.getParameters().get(0).getType(), ParameterType.TOKEN);
      assertEquals(builder.getParameters().get(1).getType(), ParameterType.TOKEN);
      assertEquals(builder.getParameters().get(2).getType(), ParameterType.TOKEN);
      assertEquals(builder.getParameters().get(3).getType(), ParameterType.TOKEN);       
      assertEquals(builder.getOrderByClause().getDirection(), null);
      assertEquals(builder.getOrderByClause().getClause(), "");
      assertEquals(builder.getWhereClause().getConditions().size(), 0);
      assertEquals(builder.getWhereClause().getClause(), "");
   }
   
   public void testInsert() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("insert into table (a, b, c, d) values (?, ?, ?, ?)");
      
      assertEquals(builder.getVerb(), Verb.INSERT);
      assertEquals(builder.getTable(), "table"); 
      assertEquals(builder.getColumns().size(), 4);
      assertEquals(builder.getColumns().get(0), "a");
      assertEquals(builder.getColumns().get(1), "b");
      assertEquals(builder.getColumns().get(2), "c");
      assertEquals(builder.getColumns().get(3), "d");      
      assertEquals(builder.getParameters().size(), 4);
      assertEquals(builder.getParameters().get(0).getColumn(), null);
      assertEquals(builder.getParameters().get(1).getColumn(), null);
      assertEquals(builder.getParameters().get(2).getColumn(), null); 
      assertEquals(builder.getParameters().get(3).getColumn(), null);      
      assertEquals(builder.getParameters().get(0).getValue(), null);
      assertEquals(builder.getParameters().get(1).getValue(), null);
      assertEquals(builder.getParameters().get(2).getValue(), null);
      assertEquals(builder.getParameters().get(3).getValue(), null);
      assertEquals(builder.getParameters().get(0).getName(), null);
      assertEquals(builder.getParameters().get(1).getName(), null);
      assertEquals(builder.getParameters().get(2).getName(), null);
      assertEquals(builder.getParameters().get(3).getName(), null);
      assertEquals(builder.getParameters().get(0).getType(), ParameterType.TOKEN);
      assertEquals(builder.getParameters().get(1).getType(), ParameterType.TOKEN);
      assertEquals(builder.getParameters().get(2).getType(), ParameterType.TOKEN);
      assertEquals(builder.getParameters().get(3).getType(), ParameterType.TOKEN);        
      assertEquals(builder.getOrderByClause().getDirection(), null);
      assertEquals(builder.getOrderByClause().getClause(), "");
      assertEquals(builder.getWhereClause().getConditions().size(), 0);
      assertEquals(builder.getWhereClause().getClause(), "");
   }
   
   public void testInsertOrIgnore() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("insert or ignore into table (a, b, c, d) values (?, ?, ?, ?)");
      
      assertEquals(builder.getVerb(), Verb.INSERT_OR_IGNORE);
      assertEquals(builder.getTable(), "table"); 
      assertEquals(builder.getColumns().size(), 4);
      assertEquals(builder.getColumns().get(0), "a");
      assertEquals(builder.getColumns().get(1), "b");
      assertEquals(builder.getColumns().get(2), "c");
      assertEquals(builder.getColumns().get(3), "d");      
      assertEquals(builder.getParameters().size(), 4);
      assertEquals(builder.getParameters().get(0).getColumn(), null);
      assertEquals(builder.getParameters().get(1).getColumn(), null);
      assertEquals(builder.getParameters().get(2).getColumn(), null); 
      assertEquals(builder.getParameters().get(3).getColumn(), null);      
      assertEquals(builder.getParameters().get(0).getValue(), null);
      assertEquals(builder.getParameters().get(1).getValue(), null);
      assertEquals(builder.getParameters().get(2).getValue(), null);
      assertEquals(builder.getParameters().get(3).getValue(), null);
      assertEquals(builder.getParameters().get(0).getName(), null);
      assertEquals(builder.getParameters().get(1).getName(), null);
      assertEquals(builder.getParameters().get(2).getName(), null);
      assertEquals(builder.getParameters().get(3).getName(), null);
      assertEquals(builder.getParameters().get(0).getType(), ParameterType.TOKEN);
      assertEquals(builder.getParameters().get(1).getType(), ParameterType.TOKEN);
      assertEquals(builder.getParameters().get(2).getType(), ParameterType.TOKEN);
      assertEquals(builder.getParameters().get(3).getType(), ParameterType.TOKEN);        
      assertEquals(builder.getOrderByClause().getDirection(), null);
      assertEquals(builder.getOrderByClause().getClause(), "");
      assertEquals(builder.getWhereClause().getConditions().size(), 0);
      assertEquals(builder.getWhereClause().getClause(), "");
   }

   public void testSelectOrderBy() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("select distinct name, address, age from table order by blah");
      
      assertEquals(builder.getVerb(), Verb.SELECT_DISTINCT);
      assertEquals(builder.getTable(), "table");      
      assertEquals(builder.getColumns().size(), 3);
      assertEquals(builder.getColumns().get(0), "name");
      assertEquals(builder.getColumns().get(1), "address");
      assertEquals(builder.getColumns().get(2), "age");
      assertEquals(builder.getParameters().size(), 0);
      assertEquals(builder.getOrderByClause().getColumn(), "blah");
      assertEquals(builder.getOrderByClause().getDirection(), null);
      assertEquals(builder.getOrderByClause().getClause(), "blah");
      assertEquals(builder.getWhereClause().getConditions().size(), 0);
      assertEquals(builder.getWhereClause().getClause(), "");
   }
   
   public void testSelectOrderByWithDirection() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("select distinct * from table order by blah asc");
      
      assertEquals(builder.getVerb(), Verb.SELECT_DISTINCT);
      assertEquals(builder.getTable(), "table"); 
      assertEquals(builder.getColumns().size(), 0);
      assertEquals(builder.getParameters().size(), 0);
      assertEquals(builder.getOrderByClause().getColumn(), "blah");
      assertEquals(builder.getOrderByClause().getDirection(), "asc");
      assertEquals(builder.getOrderByClause().getClause(), "blah asc");
      assertEquals(builder.getWhereClause().getConditions().size(), 0);
      assertEquals(builder.getWhereClause().getClause(), "");
   }
   
   public void testSelectDistinctColumns() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("select distinct a from table");
      
      assertEquals(builder.getVerb(), Verb.SELECT_DISTINCT);
      assertEquals(builder.getTable(), "table"); 
      assertEquals(builder.getColumns().size(), 1);
      assertEquals(builder.getColumns().get(0), "a");
      assertEquals(builder.getParameters().size(), 0);
      assertEquals(builder.getOrderByClause().getColumn(), null);
      assertEquals(builder.getOrderByClause().getDirection(), null);
      assertEquals(builder.getOrderByClause().getClause(), "");
      assertEquals(builder.getWhereClause().getConditions().size(), 0);
      assertEquals(builder.getWhereClause().getClause(), "");
   }
   
   public void testSelectColumns() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("select a, b,c,d, e , f from table where x == y and y == z");
      
      assertEquals(builder.getVerb(), Verb.SELECT);
      assertEquals(builder.getTable(), "table"); 
      assertEquals(builder.getColumns().size(), 6);
      assertEquals(builder.getColumns().get(0), "a");
      assertEquals(builder.getColumns().get(1), "b");
      assertEquals(builder.getColumns().get(2), "c");
      assertEquals(builder.getColumns().get(3), "d");
      assertEquals(builder.getColumns().get(4), "e");
      assertEquals(builder.getColumns().get(5), "f");
      assertEquals(builder.getParameters().size(), 0);
      assertEquals(builder.getOrderByClause().getColumn(), null);
      assertEquals(builder.getOrderByClause().getDirection(), null);
      assertEquals(builder.getOrderByClause().getClause(), "");
      assertEquals(builder.getWhereClause().getConditions().size(), 2);
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getColumn(), "x");
      assertEquals(builder.getWhereClause().getConditions().get(1).getParameter().getColumn(), "y");
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getValue(), "y");
      assertEquals(builder.getWhereClause().getConditions().get(1).getParameter().getValue(), "z");
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getName(), null);
      assertEquals(builder.getWhereClause().getConditions().get(1).getParameter().getName(), null);
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getType(), ParameterType.VALUE);
      assertEquals(builder.getWhereClause().getConditions().get(1).getParameter().getType(), ParameterType.VALUE);    
      assertEquals(builder.getWhereClause().getClause(), "x==? and y==?");
   }
   
   public void testCommandBuilder() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("select * from table where x == y and y == z");
      
      assertEquals(builder.getVerb(), Verb.SELECT);
      assertEquals(builder.getColumns().size(), 0);
      assertEquals(builder.getParameters().size(), 0);
      assertEquals(builder.getOrderByClause().getColumn(), null);
      assertEquals(builder.getOrderByClause().getDirection(), null);
      assertEquals(builder.getOrderByClause().getClause(), "");
      assertEquals(builder.getWhereClause().getConditions().size(), 2);
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getColumn(), "x");
      assertEquals(builder.getWhereClause().getConditions().get(1).getParameter().getColumn(), "y");
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getValue(), "y");
      assertEquals(builder.getWhereClause().getConditions().get(1).getParameter().getValue(), "z");
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getName(), null);
      assertEquals(builder.getWhereClause().getConditions().get(1).getParameter().getName(), null);
      assertEquals(builder.getWhereClause().getConditions().get(0).getParameter().getType(), ParameterType.VALUE);
      assertEquals(builder.getWhereClause().getConditions().get(1).getParameter().getType(), ParameterType.VALUE);    
      assertEquals(builder.getWhereClause().getClause(), "x==? and y==?");
      
     
   }

}
