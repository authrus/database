package com.authrus.database.sql;

import java.util.List;

import junit.framework.TestCase;

import com.authrus.database.sql.parse.ParameterParser;

public class ParameterParserLiteralAndTokenTest extends TestCase {

   public void testParameters() {
      ParameterParser parser = new ParameterParser("variable= :y , doo= ?,'tom', 2, 'blah  and some .....     other'");
      
      assertFalse(parser.isEmpty());
      
      List<Parameter> parameters = parser.getParameters();
      
      for(Parameter parameter : parameters) {
         System.err.println(">>"+parameter+"<<");
      }
   }
   
   public void testLiterals() {
      ParameterParser parser = new ParameterParser("variable=y , doo= ' blah ss;, ' ,'tom', 2, 'blah  and some .....     other'");
      
      assertFalse(parser.isEmpty());
      
      List<Parameter> parameters = parser.getParameters();
      
      for(Parameter parameter : parameters) {
         System.err.println(">>"+parameter+"<<");
      }
   }

}
