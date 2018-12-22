package com.authrus.database.sql.compile;

import junit.framework.TestCase;

import com.authrus.database.sql.Query;
import com.authrus.database.sql.QueryConverter;
import com.authrus.database.sql.Verb;
import com.authrus.database.sql.build.IdentityConverter;
import com.authrus.database.sql.build.QueryProcessor;

public class CreateIndexCompilerTest extends TestCase {
   
   public void testCreateTable() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("create index blah on chat (gameId, chatId)");
      
      assertEquals(builder.getVerb(), Verb.CREATE_INDEX);
      assertEquals(builder.getName(), "blah"); 
      assertEquals(builder.getTable(), "chat"); 
      assertEquals(builder.getColumns().size(), 2);    
      assertEquals(builder.getColumns().get(0), "gameId");
      assertEquals(builder.getColumns().get(1), "chatId");  
      
      CreateIndexCompiler compiler = new CreateIndexCompiler();
      String text = compiler.compile(builder, new Object[]{});      
      
      System.err.println(text);
      
      QueryConverter<Query> converter2 = new IdentityConverter();
      QueryProcessor<Query> parser2 = new QueryProcessor<Query>(converter2);
      Query builder2 = parser2.process(text);
      
      assertEquals(builder2.getVerb(), Verb.CREATE_INDEX);
      assertEquals(builder2.getName(), "blah"); 
      assertEquals(builder2.getTable(), "chat"); 
      assertEquals(builder2.getColumns().size(), 2);    
      assertEquals(builder2.getColumns().get(0), "gameId");
      assertEquals(builder2.getColumns().get(1), "chatId");   
   }

}
