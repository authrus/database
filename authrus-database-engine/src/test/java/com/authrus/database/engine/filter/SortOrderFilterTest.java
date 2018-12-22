package com.authrus.database.engine.filter;

import java.io.Serializable;

import junit.framework.TestCase;

import com.authrus.database.ResultIterator;
import com.authrus.database.bind.TableBinder;
import com.authrus.database.bind.table.attribute.AttributeTableBuilder;
import com.authrus.database.engine.Catalog;
import com.authrus.database.engine.ChangeDistributor;
import com.authrus.database.engine.ChangeListener;
import com.authrus.database.engine.LocalDatabase;

public class SortOrderFilterTest extends TestCase {

   private static class PersonRecord implements Serializable {

      private static final long serialVersionUID = 1L;
      
      public final String mail;
      public final String name;
      public final String address;
      public final int age;
      
      public PersonRecord(String mail, String name, String address, int age) {
         this.mail = mail;
         this.name = name;
         this.address = address;
         this.age = age;
      }
      
      @Override
      public String toString(){
         return String.format("%s:%s:%s:%s", mail,name,address,age);
      }
   }
   
   public void testSort() throws Exception {
      ChangeListener listener = new ChangeDistributor();
      Catalog catalog = new Catalog(listener, "test");      
      LocalDatabase store = new LocalDatabase(catalog, "test");
      AttributeTableBuilder builder = new AttributeTableBuilder(store);      
      TableBinder<PersonRecord> binder = builder.createTable("person", PersonRecord.class, "mail");
      
      assertNull(catalog.findTable("person"));      
      binder.create().execute();      
      assertNotNull(catalog.findTable("person"));  
      
      PersonRecord[] records = new PersonRecord[40];
      
      for(int i = 0; i < records.length; i++) {
         records[i] = new PersonRecord("mail"+i+"@address.com", "name-"+i, "address-"+i, i);         
      }      
      for(int i = 0; i < records.length; i++) {
         binder.insert().execute(records[i]);
      }
      ResultIterator<PersonRecord> results = binder.select().orderBy("age desc").execute();
      
      while(results.hasMore()) {
         PersonRecord record = results.next();
         System.err.println(record);
      }
   }
}
