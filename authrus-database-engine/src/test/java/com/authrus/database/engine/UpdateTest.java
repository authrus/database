package com.authrus.database.engine;

import java.io.File;
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

public class UpdateTest extends TestCase {   
   
   public static void main(String[] list) throws Exception {
      new UpdateTest().testUpdate();
      new UpdateTest().testRestore();
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
   
   public String createTempDir(String name) {
      String directory = System.getProperty("java.io.tmpdir"); 
      File newDir = new File(directory, name);
      if(!newDir.exists()) {
         newDir.mkdirs();
      }
      purge(newDir);
      return newDir.getAbsolutePath();      
   }
   
   public static void purge(File file){
      if(file.isDirectory()) {
         File[] tempFiles = file.listFiles();
         
         for(File tempFile : tempFiles) {
            if(tempFile.isFile()) {
               if(tempFile.getName().startsWith("line_test")) { // delete old test
                  tempFile.delete();
               }
            }else {
              purge(tempFile);
              tempFile.delete();               
            }
         }
      }      
   }
   
   public void testUpdate() throws Exception {
      String directory = createTempDir("test"); 
      FileLog log = new FileLog(directory, "test", 1000000, 200);
      ChangeListener listener = new ChangeLogPersister(log);
      
      log.start();
      
      Catalog catalog = new Catalog(listener, "test");
      LocalDatabase store = new LocalDatabase(catalog, "test");
      AttributeTableBuilder builder = new AttributeTableBuilder(store);      
      TableBinder<PersonRecord> binder = builder.createTable("person", PersonRecord.class, "mail");
      
      assertNull(catalog.findTable("person"));      
      binder.create().execute();      
      assertNotNull(catalog.findTable("person"));  
      
      for(int i = 0; i < 200; i++) {
         binder.insert().execute(new PersonRecord("mail"+i+"@address.com", "name-"+i, "address-"+i, i));
      }
      assertEquals(binder.select().where("mail == :mail or name == 'name-2'").set("mail", "mail3@address.com").execute().fetchAll().size(), 2);
      assertEquals(binder.select().where("mail == :mail").set("mail", "mail3@address.com").execute().fetchFirst().name, "name-3");
      assertEquals(binder.select().where("mail == :mail").set("mail", "mail3@address.com").execute().fetchFirst().address, "address-3");
      
      binder.update().execute(new PersonRecord("mail3@address.com", "Tom", "11 Street", 23));
      
      assertEquals(binder.select().where("mail == :mail").set("mail", "mail3@address.com").execute().fetchFirst().name, "Tom");
      assertEquals(binder.select().where("mail == :mail").set("mail", "mail3@address.com").execute().fetchFirst().address, "11 Street");
   }
   
   public void testRestore() throws Exception {
      String directory = createTempDir("test"); 
      FileLog log = new FileLog(directory, "test", 1000000, 200);
      ChangeListener listener = new ChangeLogPersister(log);
      
      //log.start();      
      
      Catalog catalog = new Catalog(listener, "test");
      LocalDatabase store = new LocalDatabase(catalog, "test");
      CatalogAssembler assembler = new CatalogAssembler(catalog);
      TransactionFilter filter = new RepeatFilter();
      ThreadPool pool = new ThreadPool(1);
      ChangeScheduler executor = new ThreadPoolScheduler(assembler, pool);      
      ChangeProcessor processor = new ChangeProcessor(executor, filter);
      FileSeeker seeker = new TimeFileSeeker();
      FileBlockConsumer consumer = new FileBlockConsumer(seeker, directory);
      DataRecordConsumer stream = new DataRecordConsumer(consumer);
      DataRecordIterator reader = new DataRecordIterator(stream);      
      
      consumer.start();
      processor.process(reader);
      
      AttributeTableBuilder builder = new AttributeTableBuilder(store);      
      TableBinder<PersonRecord> binder = builder.createTable("person", PersonRecord.class, "mail");
      
      assertEquals(binder.select().where("mail == :mail").set("mail", "mail3@address.com").execute().fetchFirst().name, "Tom");
      assertEquals(binder.select().where("mail == :mail").set("mail", "mail3@address.com").execute().fetchFirst().address, "11 Street");
   }
}
