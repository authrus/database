package com.authrus.database.sql;


import com.authrus.database.sql.Condition;
import com.authrus.database.sql.parse.ConditionParser;

import junit.framework.TestCase;

public class ConditionParserTest extends TestCase {

   public void testConditions() {
      assertCondition("x==y", "x", null, "y", "==", ParameterType.VALUE);
      assertCondition("x like y", "x", null, "y", "like", ParameterType.VALUE);
      assertCondition("x ~= y", "x", null, "y", "~=", ParameterType.VALUE);        
      assertCondition("x == y", "x", null, "y", "==", ParameterType.VALUE);
      assertCondition("x>y", "x", null, "y", ">", ParameterType.VALUE);
      assertCondition("x!=y", "x", null, "y", "!=", ParameterType.VALUE);
      assertCondition("x!=  y  ", "x", null, "y", "!=", ParameterType.VALUE);
      assertCondition(" x <>y", "x", null, "y", "<>", ParameterType.VALUE);
      assertCondition("name !=:name", "name", "name", null, "!=", ParameterType.NAME);
      assertCondition("blah==?", "blah", null, null, "==", ParameterType.TOKEN);
      assertCondition(" x<>:someVariable", "x", "someVariable", null, "<>", ParameterType.NAME);
      assertCondition("x>1", "x", null, "1", ">", ParameterType.VALUE);
      assertCondition("x<=10.0f", "x", null, "10.0f", "<=", ParameterType.VALUE);
      assertCondition("text!=\"blah\"", "text", null, "blah", "!=", ParameterType.VALUE);
      assertCondition("description == 'some test description'", "description", null, "some test description", "==", ParameterType.VALUE);
      assertCondition("x=='x ,6 ,7,s,d,'", "x", null, "x ,6 ,7,s,d,", "==", ParameterType.VALUE);    
   }

   public void assertCondition(String source, String column, String name, String value, String operator, ParameterType type) {
      ConditionParser parser = new ConditionParser();

      if (source != null) {
         parser.parse(source);
         
         Condition condition = parser.getCondition();

         assertEquals(condition.getParameter().getColumn(), column);
         assertEquals(condition.getParameter().getName(), name);
         assertEquals(condition.getParameter().getValue(), value);
         assertEquals(condition.getParameter().getType(), type);
         assertEquals(condition.getComparison(), operator);
      }
   }
}
