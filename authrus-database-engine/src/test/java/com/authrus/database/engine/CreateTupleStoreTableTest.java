package com.authrus.database.engine;

import junit.framework.TestCase;

import com.authrus.database.Database;
import com.authrus.database.DatabaseConnection;
import com.authrus.database.Record;
import com.authrus.database.ResultIterator;
import com.authrus.database.Statement;
import com.authrus.database.data.DataType;
import com.authrus.database.engine.Catalog;
import com.authrus.database.engine.ChangeDistributor;
import com.authrus.database.engine.LocalDatabase;

public class CreateTupleStoreTableTest extends TestCase {

   private static final String DROP_TABLE = 
   "DROP TABLE IF EXISTS person";

   private static final String CREATE_TABLE = 
   "CREATE TABLE IF NOT EXISTS person (\n" +
   "  id INT NOT NULL,\n"+
   "  name TEXT NOT NULL,\n"+
   "  address TEXT,\n"+
   "  age INT,\n"+
   "  job TEXT,\n"+
   "  PRIMARY KEY(id)\n"+
   ")";
   
   private static final String CREATE_INDEX = 
   "CREATE INDEX name_idx ON person (name)";
   
   public void testCreateTable() throws Exception {
      ChangeDistributor distributor = new ChangeDistributor();
      Catalog catalog = new Catalog(distributor, "test");
      Database store = new LocalDatabase(catalog, "test");
      DatabaseConnection connection = store.getConnection(); ;
      
      assertNull(catalog.findTable("person"));
      
      connection.executeStatement(CREATE_TABLE);
      
      assertNotNull(catalog.findTable("person"));
      assertEquals(catalog.findTable("person").getSchema().getCount(), 5);
      assertEquals(catalog.findTable("person").getSchema().getColumn(0).getName(), "id");
      assertEquals(catalog.findTable("person").getSchema().getColumn(0).getDataType(), DataType.INT);
      assertEquals(catalog.findTable("person").getSchema().getColumn(1).getName(), "name");
      assertEquals(catalog.findTable("person").getSchema().getColumn(1).getDataType(), DataType.TEXT);
      assertEquals(catalog.findTable("person").getSchema().getColumn(2).getName(), "address");
      assertEquals(catalog.findTable("person").getSchema().getColumn(2).getDataType(), DataType.TEXT);
      assertEquals(catalog.findTable("person").getSchema().getColumn(3).getName(), "age");
      assertEquals(catalog.findTable("person").getSchema().getColumn(3).getDataType(), DataType.INT);
      assertEquals(catalog.findTable("person").getSchema().getColumn(4).getName(), "job");      
      assertEquals(catalog.findTable("person").getSchema().getColumn(4).getDataType(), DataType.TEXT);
      
      connection.executeStatement("INSERT INTO person (id, name, address, age, job) values (1, 'Tom', '1 Some Place', 22, 'Trader')");
      connection.executeStatement("INSERT INTO person (id, name, address, age, job) values (2, 'Sam', '22 Some Place', 32, 'Shop Keeper')");
      connection.executeStatement("INSERT INTO person (id, name, address, age, job) values (3, 'Bobby', '233 Some Place', 23, 'Dole Head')");
      
      Statement statement = connection.prepareStatement("SELECT name FROM person WHERE age <= 22");
      ResultIterator<Record> iterator = statement.execute();
      
      assertFalse(iterator.isEmpty());
      assertEquals(iterator.fetchFirst().getString("name"), "Tom");
      
      connection.executeStatement(DROP_TABLE);
      
      assertNull(catalog.findTable("person"));
      
      connection.executeStatement(CREATE_TABLE);
      
      assertNotNull(catalog.findTable("person"));
      assertEquals(catalog.findTable("person").getSchema().getCount(), 5);
      assertEquals(catalog.findTable("person").getSchema().getColumn(0).getName(), "id");
      assertEquals(catalog.findTable("person").getSchema().getColumn(0).getDataType(), DataType.INT);
      assertEquals(catalog.findTable("person").getSchema().getColumn(1).getName(), "name");
      assertEquals(catalog.findTable("person").getSchema().getColumn(1).getDataType(), DataType.TEXT);
      assertEquals(catalog.findTable("person").getSchema().getColumn(2).getName(), "address");
      assertEquals(catalog.findTable("person").getSchema().getColumn(2).getDataType(), DataType.TEXT);
      assertEquals(catalog.findTable("person").getSchema().getColumn(3).getName(), "age");
      assertEquals(catalog.findTable("person").getSchema().getColumn(3).getDataType(), DataType.INT);
      assertEquals(catalog.findTable("person").getSchema().getColumn(4).getName(), "job");      
      assertEquals(catalog.findTable("person").getSchema().getColumn(4).getDataType(), DataType.TEXT);
      
      connection.executeStatement(CREATE_INDEX);
      
      connection.executeStatement("INSERT INTO person (id, name, address, age, job) values (1, 'Tom', '1 Some Place', 22, 'Trader')");
      connection.executeStatement("INSERT INTO person (id, name, address, age, job) values (2, 'Sam', '22 Some Place', 32, 'Shop Keeper')");
      connection.executeStatement("INSERT INTO person (id, name, address, age, job) values (3, 'Bobby', '233 Some Place', 23, 'Dole Head')");
      
      statement = connection.prepareStatement("SELECT name FROM person WHERE age <= 22");
      iterator = statement.execute();
      
      assertFalse(iterator.isEmpty());
      assertEquals(iterator.fetchFirst().getString("name"), "Tom");
   }
}
