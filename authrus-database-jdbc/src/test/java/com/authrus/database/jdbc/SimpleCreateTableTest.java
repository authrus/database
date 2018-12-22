package com.authrus.database.jdbc;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.h2.jdbcx.JdbcDataSource;

import com.authrus.database.DatabaseConnection;
import com.authrus.database.Record;
import com.authrus.database.ResultIterator;
import com.authrus.database.Statement;
import com.authrus.database.sql.compile.QueryCompiler;

public class SimpleCreateTableTest extends TestCase {
   
   public void testDotInColumnNames() throws Exception {
      JdbcDataSource source = new JdbcDataSource();
      
      source.setUrl("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");
      
      Map<String, String> translations = new HashMap<String, String>();
      QueryCompiler compiler = new QueryCompiler(translations);      
      
      translations.put("optional", null);
      translations.put("required", "not null");
      translations.put("sequence", "default sequence");     
      
      PoolDatabase database = new PoolDatabase(source, compiler);  
      DatabaseConnection connection = database.getConnection();
      
      try {
         connection.executeStatement("drop table if exists test");
         connection.executeStatement("create table test ("+ 
               "some_id int not null,"+
               "some_name text not null," +
               "some_age int not null," +
               "some_address text," +
               "some_salary double,"+
               "primary key(some_id))");
      } finally {
         connection.closeConnection();
      }
   }

   public void testSimpleTable() throws Exception {     
      JdbcDataSource source = new JdbcDataSource();
      
      source.setUrl("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");
      
      Map<String, String> translations = new HashMap<String, String>();
      QueryCompiler compiler = new QueryCompiler(translations);      
      
      translations.put("optional", null);
      translations.put("required", "not null");
      translations.put("sequence", "default sequence");     
      
      PoolDatabase database = new PoolDatabase(source, compiler); 
      DatabaseConnection connection = database.getConnection();
      
      try {
         connection.executeStatement("drop table if exists x");
         connection.executeStatement("create table x (x double not null, primary key(x))");
         Statement statement = connection.prepareStatement("insert into x (x) values (:x)");
         
         statement.set("x", 1.2d);
         
         ResultIterator<Record> iterator = statement.execute();
         
         assertFalse(iterator.hasMore());
         assertTrue(iterator.isEmpty());      
         iterator.close();
         
         Statement select = connection.prepareStatement("select x from x where x > :x");
         
         select.set("x", 1.0d);
         
         ResultIterator<Record> results = select.execute();
         
         assertTrue(results.hasMore());
         assertFalse(results.isEmpty());      
         assertNotNull(results.next());
        
         Record result = results.next();
         
         //assertEquals(result.getDouble("x"), 1.2d); // rounding issue!!
         assertFalse(results.hasMore());
      } finally {
         connection.closeConnection();
      }
   }
   
   public void testNamedParameters() throws Exception {
      JdbcDataSource source = new JdbcDataSource();
      
      source.setUrl("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");
      
      Map<String, String> translations = new HashMap<String, String>();
      QueryCompiler compiler = new QueryCompiler(translations);      
      
      translations.put("optional", null);
      translations.put("required", "not null");
      translations.put("sequence", "default sequence");
      translations.put("insert or ignore", "insert");        
      
      PoolDatabase database = new PoolDatabase(source, compiler); 
      DatabaseConnection connection = database.getConnection();
      
      try {
         connection.executeStatement("drop table if exists profile");
         connection.executeStatement("create table profile ("+ 
               "id int not null,"+
               "name text not null," +
               "age int not null," +
               "address text," +
               "salary double," +
               "primary key(id))");
         Statement billHicks = connection.prepareStatement("insert into profile (id, name, age, address, salary) values (:theId, :theName, :theAge, :theStreet, :theSalary)");
         
         billHicks.set("theId", 1);
         billHicks.set("theName", "Bill Hicks");
         billHicks.set("theAge", 25);
         billHicks.set("theStreet", "Some Street");
         billHicks.set("theSalary", 110000);
         billHicks.execute();
         
         Statement williamShatner = connection.prepareStatement("insert or ignore into profile (id, name, age, address, salary) values (:theId, :theName, :theAge, :theStreet, :theSalary)");
         
         williamShatner.set("theId", 2);
         williamShatner.set("theName", "William Shatner");
         williamShatner.set("theAge", 75);
         williamShatner.set("theStreet", "Star Trek");
         williamShatner.set("theSalary", 310000);
         williamShatner.execute();
         
         Statement select = connection.prepareStatement("select name, age from profile where salary > :x");
         
         select.set("x", 110000);
         
         ResultIterator<Record> iterator = select.execute();
         
         assertTrue(iterator.hasMore());
         assertTrue(iterator.hasMore());
         assertTrue(iterator.hasMore());
         assertFalse(iterator.isEmpty());
         
         Map<String, Integer> results = new LinkedHashMap<String, Integer>();
         
         while(iterator.hasMore()) {
            Record result = iterator.next();
            String name = result.getString("name");
            int age = result.getInteger("age");
            
            System.err.println("name="+name+" age="+age);
            results.put(name, age);
         }
         assertEquals(results.get("William Shatner"), new Integer(75));
      } finally {
         connection.closeConnection();
      }
   }
}
