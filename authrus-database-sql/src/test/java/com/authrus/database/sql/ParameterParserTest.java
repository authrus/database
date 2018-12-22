package com.authrus.database.sql;
import static com.authrus.database.sql.ParameterType.*;
import java.util.List;

import junit.framework.TestCase;

import com.authrus.database.sql.Parameter;
import com.authrus.database.sql.parse.ParameterParser;

public class ParameterParserTest extends TestCase {

   public void testParameters() {
      assertParameter("?", null, null, null, TOKEN);
      assertParameter(":name", null, "name", null, NAME);
      assertParameter("x=:name", "x", "name", null, NAME);
      assertParameter("x=?", "x", null, null, TOKEN);
      assertParameter("'hello world'", null, null, "hello world", VALUE);
      assertParameter("x =  'hello world'", "y", null, "hello world", VALUE);            
   }

   public void assertParameter(String source, String column, String name, String value, ParameterType type) {
      ParameterParser parser = new ParameterParser();

      if (source != null) {
         parser.parse(source);
         
         assertFalse(parser.isEmpty());
         
         List<Parameter> parameters = parser.getParameters();
         Parameter parameter = parameters.get(0);

         assertEquals(parameter.getName(), name);
         assertEquals(parameter.getValue(), value);
         assertEquals(parameter.getType(), type);
      }
   }
}
