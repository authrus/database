package com.authrus.database.bind.table.reflect;

import java.io.Serializable;
import java.util.Date;

import junit.framework.TestCase;

import com.authrus.database.Column;
import com.authrus.database.PrimaryKey;
import com.authrus.database.Schema;
import com.authrus.database.attribute.ObjectBuilder;
import com.authrus.database.bind.table.attribute.AttributeSchemaScanner;
import com.authrus.database.bind.table.attribute.RowBuilder;

public class SchemaScannerTest extends TestCase {
   
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
   
   public void testSchemaScanner() throws Exception {
      ObjectBuilder builder = new RowBuilder();
      AttributeSchemaScanner scanner = new AttributeSchemaScanner(builder);
      Schema schema = scanner.createSchema(ExampleTable.class, "time");
      int count = schema.getCount();
      
      assertEquals(count, 5);
      assertNotNull(schema.getColumn("time"));
      assertEquals(schema.getColumn("time").getDataType().getType(), Long.class);
      assertNotNull(schema.getColumn("date"));
      assertEquals(schema.getColumn("date").getDataType().getType(), Long.class);
      assertNotNull(schema.getColumn("childName"));
      assertEquals(schema.getColumn("childName").getDataType().getType(), String.class);
      assertNotNull(schema.getColumn("childAge"));
      assertEquals(schema.getColumn("childAge").getDataType().getType(), Integer.class);
      assertNotNull(schema.getColumn("childAddress"));
      assertEquals(schema.getColumn("childAddress").getDataType().getType(), String.class);
      
      for(int i = 0; i < count; i++) {
         Column column = schema.getColumn(i);
         System.err.printf("name='%s' title='%s' type='%s' index='%s'%n", column.getName(), column.getTitle(), column.getDataType(), column.getIndex());
      }
      PrimaryKey key = schema.getKey();
      
      assertEquals(key.getCount(), 1);
      assertEquals(key.getColumn(0).getName(), "time");
      assertEquals(key.getColumn(0).getDataType().getType(), Long.class);      
      
   }

}
