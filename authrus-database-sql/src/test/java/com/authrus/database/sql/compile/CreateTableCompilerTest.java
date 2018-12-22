package com.authrus.database.sql.compile;

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import com.authrus.database.data.DataConstraint;
import com.authrus.database.data.DataType;
import com.authrus.database.sql.Query;
import com.authrus.database.sql.QueryConverter;
import com.authrus.database.sql.Verb;
import com.authrus.database.sql.build.IdentityConverter;
import com.authrus.database.sql.build.QueryProcessor;

public class CreateTableCompilerTest extends TestCase {
   
   public void testCreateTable() throws Exception {
      QueryConverter<Query> converter = new IdentityConverter();
      QueryProcessor<Query> parser = new QueryProcessor<Query>(converter);
      Query builder = parser.process("create table if not exists chat (chatId int default sequence, gameId int not null, message text, primary key(chatId, gameId))");
      
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
      assertEquals(builder.getCreateSchema().getColumn(0).getDataConstraint(), DataConstraint.OPTIONAL);
      assertEquals(builder.getCreateSchema().getColumn(1).getName(), "gameId");
      assertEquals(builder.getCreateSchema().getColumn(1).getDataType(), DataType.INT);
      assertEquals(builder.getCreateSchema().getColumn(1).getDataConstraint(), DataConstraint.REQUIRED);
      assertEquals(builder.getCreateSchema().getColumn(2).getName(), "message");
      assertEquals(builder.getCreateSchema().getColumn(2).getDataType(), DataType.TEXT);
      assertEquals(builder.getCreateSchema().getColumn(2).getDataConstraint(), DataConstraint.OPTIONAL); 
      
      Map<String, String> translations = new HashMap<String, String>();
      CreateTableCompiler compiler = new CreateTableCompiler(translations);
      
      translations.put("optional", null);
      translations.put("required", "not null");
      translations.put("sequence", "default sequence");      
      
      String text = compiler.compile(builder, new Object[]{});      
      
      System.err.println(text);
      
      QueryConverter<Query> converter2 = new IdentityConverter();
      QueryProcessor<Query> parser2 = new QueryProcessor<Query>(converter2);
      Query builder2 = parser2.process(text);
      
      assertEquals(builder2.getVerb(), Verb.CREATE_TABLE);  
      assertEquals(builder2.getTable(), "chat"); 
      assertEquals(builder2.getColumns().size(), 0);     
      assertEquals(builder2.getParameters().size(), 0);
      assertEquals(builder2.getCreateSchema().getKey().getColumns().size(), 2);
      assertEquals(builder2.getCreateSchema().getKey().getColumns().get(0), "chatId");
      assertEquals(builder2.getCreateSchema().getKey().getColumns().get(1), "gameId");      
      assertEquals(builder2.getCreateSchema().getColumns().size(), 3);
      assertEquals(builder2.getCreateSchema().getColumn(0).getName(), "chatId");
      assertEquals(builder2.getCreateSchema().getColumn(0).getDataType(), DataType.INT);
      assertEquals(builder2.getCreateSchema().getColumn(0).getDataConstraint(), DataConstraint.OPTIONAL);
      assertEquals(builder2.getCreateSchema().getColumn(1).getName(), "gameId");
      assertEquals(builder2.getCreateSchema().getColumn(1).getDataType(), DataType.INT);
      assertEquals(builder2.getCreateSchema().getColumn(1).getDataConstraint(), DataConstraint.REQUIRED);
      assertEquals(builder2.getCreateSchema().getColumn(2).getName(), "message");
      assertEquals(builder2.getCreateSchema().getColumn(2).getDataType(), DataType.TEXT);
      assertEquals(builder2.getCreateSchema().getColumn(2).getDataConstraint(), DataConstraint.OPTIONAL);        
   }

}
