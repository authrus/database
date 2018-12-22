package com.authrus.database.sql;

import junit.framework.TestCase;

import com.authrus.database.data.DataConstraint;
import com.authrus.database.data.DataType;
import com.authrus.database.function.DefaultFunction;
import com.authrus.database.sql.build.IdentityConverter;
import com.authrus.database.sql.build.QueryProcessor;

public class CreateTest extends TestCase {
   
   public void testDropTable() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("drop table x");
      
      assertEquals(builder.getVerb(), Verb.DROP_TABLE);
      assertEquals(builder.getName(), null); 
      assertEquals(builder.getTable(), "x"); 
      assertEquals(builder.getColumns().size(), 0);    
   }
   
   public void testDropTableIfExists() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("drop table if exists x");
      
      assertEquals(builder.getVerb(), Verb.DROP_TABLE);
      assertEquals(builder.getName(), null); 
      assertEquals(builder.getTable(), "x"); 
      assertEquals(builder.getColumns().size(), 0);    
   }
   
   public void testDropIndex() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("drop index x");
      
      assertEquals(builder.getVerb(), Verb.DROP_INDEX);
      assertEquals(builder.getName(), "x"); 
      assertEquals(builder.getTable(), null); 
      assertEquals(builder.getColumns().size(), 0);    
   }
   
   public void testDropIndexIfExists() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("drop index if exists x");
      
      assertEquals(builder.getVerb(), Verb.DROP_INDEX);
      assertEquals(builder.getName(), "x"); 
      assertEquals(builder.getTable(), null); 
      assertEquals(builder.getColumns().size(), 0);    
   }
   
   public void testCreateIndex() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("create index blah on chat (gameId, chatId)");
      
      assertEquals(builder.getVerb(), Verb.CREATE_INDEX);
      assertEquals(builder.getName(), "blah"); 
      assertEquals(builder.getTable(), "chat"); 
      assertEquals(builder.getColumns().size(), 2);    
      assertEquals(builder.getColumns().get(0), "gameId");
      assertEquals(builder.getColumns().get(1), "chatId");  
   }
   
   public void testCreateIndexSingleColumn() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("create index blah on chat (chatId)");
      
      assertEquals(builder.getVerb(), Verb.CREATE_INDEX);
      assertEquals(builder.getName(), "blah"); 
      assertEquals(builder.getTable(), "chat"); 
      assertEquals(builder.getColumns().size(), 1);    
      assertEquals(builder.getColumns().get(0), "chatId");  
   }
   
   public void testCreateIndexIfNotExists() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("create index if not exists a on b (x, y, z)");
      
      assertEquals(builder.getVerb(), Verb.CREATE_INDEX);
      assertEquals(builder.getName(), "a"); 
      assertEquals(builder.getTable(), "b"); 
      assertEquals(builder.getColumns().size(), 3);    
      assertEquals(builder.getColumns().get(0), "x");
      assertEquals(builder.getColumns().get(1), "y"); 
      assertEquals(builder.getColumns().get(2), "z"); 
   }   

   public void testCreateTable() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("create table x(a int, b double, c date, primary key(a))");
      
      assertEquals(builder.getVerb(), Verb.CREATE_TABLE);  
      assertEquals(builder.getTable(), "x"); 
      assertEquals(builder.getColumns().size(), 0);     
      assertEquals(builder.getParameters().size(), 0);
      assertEquals(builder.getCreateSchema().getKey().getColumns().size(), 1);
      assertEquals(builder.getCreateSchema().getKey().getColumns().get(0), "a");
      assertEquals(builder.getCreateSchema().getColumn(0).getName(), "a");
      assertEquals(builder.getCreateSchema().getColumn(0).getDataType(), DataType.INT);
      assertEquals(builder.getCreateSchema().getColumn(0).getDataConstraint(), DataConstraint.OPTIONAL);       
      assertEquals(builder.getCreateSchema().getColumn(1).getName(), "b");
      assertEquals(builder.getCreateSchema().getColumn(1).getDataType(), DataType.DOUBLE);
      assertEquals(builder.getCreateSchema().getColumn(1).getDataConstraint(), DataConstraint.OPTIONAL);          
      assertEquals(builder.getCreateSchema().getColumn(2).getName(), "c");
      assertEquals(builder.getCreateSchema().getColumn(2).getDataType(), DataType.DATE);    
      assertEquals(builder.getCreateSchema().getColumn(2).getDataConstraint(), DataConstraint.OPTIONAL);   
   }
   
   public void testCreateTableIfNotExists() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("create table if not exists chat (chatId int not null, gameId int not null, message text, primary key(chatId))");
      
      assertEquals(builder.getVerb(), Verb.CREATE_TABLE);  
      assertEquals(builder.getTable(), "chat"); 
      assertEquals(builder.getColumns().size(), 0);     
      assertEquals(builder.getParameters().size(), 0);
      assertEquals(builder.getCreateSchema().getKey().getColumns().size(), 1);
      assertEquals(builder.getCreateSchema().getKey().getColumns().get(0), "chatId");
      assertEquals(builder.getCreateSchema().getColumns().size(), 3);
      assertEquals(builder.getCreateSchema().getColumn(0).getName(), "chatId");
      assertEquals(builder.getCreateSchema().getColumn(0).getDataType(), DataType.INT);
      assertEquals(builder.getCreateSchema().getColumn(0).getDataConstraint(), DataConstraint.REQUIRED);       
      assertEquals(builder.getCreateSchema().getColumn(1).getName(), "gameId");
      assertEquals(builder.getCreateSchema().getColumn(1).getDataType(), DataType.INT);
      assertEquals(builder.getCreateSchema().getColumn(1).getDataConstraint(), DataConstraint.REQUIRED);          
      assertEquals(builder.getCreateSchema().getColumn(2).getName(), "message");
      assertEquals(builder.getCreateSchema().getColumn(2).getDataType(), DataType.TEXT);    
      assertEquals(builder.getCreateSchema().getColumn(2).getDataConstraint(), DataConstraint.OPTIONAL);   
   }
   
   public void testCreateTableCompositeKey() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("create table if not exists chat (chatId int not null, gameId int not null, message text, primary key(chatId, gameId))");
      
      assertEquals(builder.getVerb(), Verb.CREATE_TABLE);  
      assertEquals(builder.getTable(), "chat"); 
      assertEquals(builder.getColumns().size(), 0);     
      assertEquals(builder.getParameters().size(), 0);
      assertEquals(builder.getCreateSchema().getKey().getColumns().size(), 2);
      assertEquals(builder.getCreateSchema().getKey().getColumns().get(0), "chatId");
      assertEquals(builder.getCreateSchema().getKey().getColumns().get(1), "gameId");      
      assertEquals(builder.getCreateSchema().getColumns().size(), 3);
      assertEquals(builder.getCreateSchema().getColumn(0).getName(), "chatId");
      assertEquals(builder.getCreateSchema().getColumn(0).getDataType(), DataType.INT);
      assertEquals(builder.getCreateSchema().getColumn(0).getDataConstraint(), DataConstraint.REQUIRED);       
      assertEquals(builder.getCreateSchema().getColumn(1).getName(), "gameId");
      assertEquals(builder.getCreateSchema().getColumn(1).getDataType(), DataType.INT);
      assertEquals(builder.getCreateSchema().getColumn(1).getDataConstraint(), DataConstraint.REQUIRED);          
      assertEquals(builder.getCreateSchema().getColumn(2).getName(), "message");
      assertEquals(builder.getCreateSchema().getColumn(2).getDataType(), DataType.TEXT);    
      assertEquals(builder.getCreateSchema().getColumn(2).getDataConstraint(), DataConstraint.OPTIONAL);   
   }
   
   public void testCreateTableDefaultCurrentTime() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("create table if not exists chat (chatId int not null default time, gameId int not null, message text, primary key(chatId, gameId))");
      
      assertEquals(builder.getVerb(), Verb.CREATE_TABLE);  
      assertEquals(builder.getTable(), "chat"); 
      assertEquals(builder.getColumns().size(), 0);     
      assertEquals(builder.getParameters().size(), 0);
      assertEquals(builder.getCreateSchema().getKey().getColumns().size(), 2);
      assertEquals(builder.getCreateSchema().getKey().getColumns().get(0), "chatId");
      assertEquals(builder.getCreateSchema().getKey().getColumns().get(1), "gameId");      
      assertEquals(builder.getCreateSchema().getColumns().size(), 3);
      assertEquals(builder.getCreateSchema().getColumn(0).getName(), "chatId");
      assertEquals(builder.getCreateSchema().getColumn(0).getDataType(), DataType.INT);
      assertEquals(builder.getCreateSchema().getColumn(0).getDataConstraint(), DataConstraint.REQUIRED);
      assertEquals(builder.getCreateSchema().getColumn(0).getDefaultValue().getExpression(), "time");
      assertEquals(builder.getCreateSchema().getColumn(0).getDefaultValue().getFunction(), DefaultFunction.CURRENT_TIME);       
      assertEquals(builder.getCreateSchema().getColumn(1).getName(), "gameId");
      assertEquals(builder.getCreateSchema().getColumn(1).getDataType(), DataType.INT);
      assertEquals(builder.getCreateSchema().getColumn(1).getDataConstraint(), DataConstraint.REQUIRED);          
      assertEquals(builder.getCreateSchema().getColumn(2).getName(), "message");
      assertEquals(builder.getCreateSchema().getColumn(2).getDataType(), DataType.TEXT);    
      assertEquals(builder.getCreateSchema().getColumn(2).getDataConstraint(), DataConstraint.OPTIONAL);   
   }
}
