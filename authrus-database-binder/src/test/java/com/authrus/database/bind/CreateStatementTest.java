package com.authrus.database.bind;

import java.io.Serializable;
import java.util.Date;

import junit.framework.TestCase;

import com.authrus.database.bind.table.TableBuilder;
import com.authrus.database.bind.table.attribute.AttributeTableBuilder;
import com.authrus.database.bind.table.statement.CreateStatement;

public class CreateStatementTest extends TestCase {
   
   public static class ExampleChild implements Serializable{
      String name;
      int age;
      String address;
   }
   public static class ExampleTable implements Serializable{
      ExampleChild child;
      Date date;
      long time;
   }   

   public void testCreateStatement() throws Exception {
      TableBuilder scanner = new AttributeTableBuilder(null);
      TableBinder table = scanner.createTable("example", ExampleTable.class, "time");       		
      CreateStatement statement = table.create();
      String value = statement.compile();
      
      System.err.println(value);
   }

}
