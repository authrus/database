package com.authrus.database.engine;

import java.io.Serializable;

import junit.framework.TestCase;

import com.authrus.database.bind.TableBinder;
import com.authrus.database.bind.table.attribute.AttributeTableBuilder;
import com.authrus.database.common.thread.ThreadPool;
import com.authrus.database.engine.io.DataRecordConsumer;
import com.authrus.database.engine.io.DataRecordIterator;
import com.authrus.database.engine.io.FileBlockConsumer;
import com.authrus.database.engine.io.FileSeeker;
import com.authrus.database.engine.io.TimeFileSeeker;
import com.authrus.database.engine.io.read.CatalogAssembler;
import com.authrus.database.engine.io.read.ChangeProcessor;
import com.authrus.database.engine.io.read.ChangeScheduler;
import com.authrus.database.engine.io.read.RepeatFilter;
import com.authrus.database.engine.io.read.ThreadPoolScheduler;
import com.authrus.database.engine.io.write.ChangeLogPersister;
import com.authrus.database.engine.io.write.FileLog;

public class ReadWritePerformanceTest extends TestCase {
   
   private static final int ITERATIONS = 1000000;
   
   public static void main(String[] list) throws Exception {
      new ReadWritePerformanceTest().setUp();      
      new ReadWritePerformanceTest().testWritePerformance();
      new ReadWritePerformanceTest().testReadPerformance();      
   }
   
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
   
   public void testWritePerformance() throws Exception {
      String directory = System.getProperty("java.io.tmpdir"); 
      FileLog log = new FileLog(directory, "test", 1000000, 1000);
      ChangeListener listener = new ChangeLogPersister(log);
      
      log.start();
      
      Catalog catalog = new Catalog(listener, "test");      
      LocalDatabase store = new LocalDatabase(catalog, "test");
      AttributeTableBuilder builder = new AttributeTableBuilder(store);      
      TableBinder<PersonRecord> binder = builder.createTable("person", PersonRecord.class, "mail");
      
      assertNull(catalog.findTable("person"));      
      binder.create().execute();      
      assertNotNull(catalog.findTable("person"));  
      
      PersonRecord[] record = new PersonRecord[ITERATIONS];
      
      for(int i = 0; i < ITERATIONS; i++) {
         record[i] = new PersonRecord("mail"+i+"@address.com", "name-"+i, "address-"+i, i);         
      }
      long start = System.currentTimeMillis();
      
      for(int i = 0; i < ITERATIONS; i++) {
         binder.insert().execute(record[i]);  
         if(i % 1000 == 0) {
            System.err.println(i);
         }
      }
      long end = System.currentTimeMillis();
      long time = Math.min(end - start, 1);      
      
      System.err.println("INSERT:" + time + " ms which is " +(ITERATIONS/1000)+" per ms");
      System.err.flush();
      
      Thread.sleep(5000);
   }
   
   public void testReadPerformance() throws Exception {      
      String directory = System.getProperty("java.io.tmpdir"); 
      FileLog log = new FileLog(directory, "test", 1000000, 1000);
      ChangeListener listener = new ChangeLogPersister(log);
      
      //log.start();
      
      Catalog catalog = new Catalog(listener, "test");
      LocalDatabase store = new LocalDatabase(catalog, "test");
      AttributeTableBuilder builder = new AttributeTableBuilder(store);      
      TableBinder<PersonRecord> binder = builder.createTable("person", PersonRecord.class, "mail");  
      
      long start = System.currentTimeMillis();
      
      TransactionFilter filter = new RepeatFilter();
      ThreadPool pool = new ThreadPool(1);
      CatalogAssembler assembler = new CatalogAssembler(catalog);
      ChangeScheduler executor = new ThreadPoolScheduler(assembler, pool);
      ChangeProcessor processor = new ChangeProcessor(executor, filter);
      FileSeeker seeker = new TimeFileSeeker();
      FileBlockConsumer consumer = new FileBlockConsumer(seeker, directory);
      DataRecordConsumer stream = new DataRecordConsumer(consumer);
      DataRecordIterator reader = new DataRecordIterator(stream);
        
      consumer.start();
      processor.process(reader);
      
      long end = System.currentTimeMillis();
      long time = Math.min(end - start, 1);      
      
      System.err.println("READ:" + time + " ms which is " +(ITERATIONS/1000)+" per ms");
      
      assertEquals(binder.select().where("name == 'name-123' and address == 'address-123'").execute().fetchFirst().mail, "mail123@address.com");
      assertEquals(binder.select().where("name == 'name-123' and address == 'address-123'").execute().fetchFirst().age, 123);
      
      for(int i = 0; i < 10; i++) {
         start = System.currentTimeMillis();
         System.err.println(binder.select().where("name == 'name-922123' and address != 'address-2'").execute().fetchFirst());
         end = System.currentTimeMillis();
         time = end - start;
         
         System.err.println("FIND:" + time + " ms");
      }      
   }

}
