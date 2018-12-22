package com.authrus.database.engine;

import java.io.Serializable;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

import com.authrus.database.bind.TableBinder;
import com.authrus.database.bind.table.attribute.AttributeTableBuilder;
import com.authrus.database.engine.Catalog;
import com.authrus.database.engine.ChangeDistributor;
import com.authrus.database.engine.LocalDatabase;

public class TupleStoreTest extends TestCase {
   
   private static final int ITERATIONS = 1000000;
   
   public static void main(String[] list) throws Exception {
      new TupleStoreTest().testTupleStore();
   }
   
   private static class ExampleObject implements Serializable {

      private static final long serialVersionUID = 1L;
      
      public final String key;
      public final String name;
      public final String address;
      public final int age;
      
      public ExampleObject(String key, String name, String address, int age) {
         this.key = key;
         this.name = name;
         this.address = address;
         this.age = age;
      }
      
      @Override
      public String toString(){
         return String.format("%s:%s:%s:%s", key,name,address,age);
      }
   }
   
   public void testTupleStore() throws Exception {
      ChangeDistributor distributor = new ChangeDistributor();
      Catalog catalog = new Catalog(distributor, "test");    
      LocalDatabase store = new LocalDatabase(catalog, "test");
      AttributeTableBuilder builder = new AttributeTableBuilder(store);      
      TableBinder binder = builder.createTable("example", ExampleObject.class, "key");
      
      assertNull(catalog.findTable("example"));
      
      binder.create().execute();
      
      assertNotNull(catalog.findTable("example"));
      
      long start = System.currentTimeMillis();
      
      for(int i = 0; i < ITERATIONS; i++) {
         binder.insert().execute(new ExampleObject("key-"+i, "name-"+i, "address-"+i, i));
         //if(i % 100 == 0){
         //   System.err.println(i + " at " + (System.currentTimeMillis()-start));
        // }
      }
      long end = System.currentTimeMillis();
      long time = end - start;
      
      System.err.println("INSERT:" + time + " ms which is " +(ITERATIONS/(time/1000))+" per second");
      
      start = System.currentTimeMillis();
      
      for(int i = 0; i < 1000; i++) {
         long begin = System.currentTimeMillis();
         List<ExampleObject> results = binder.select()
            .where("key == 'key-0' or key == 'key-5'")
            .execute()
            .fetchAll();
         
         System.err.println("Took " + (System.currentTimeMillis() - begin) + " to do " + ITERATIONS + " rows");
         assertFalse(results.isEmpty());
         assertEquals(results.size(), 2);
      }
      end = System.currentTimeMillis();
      time = end - start;
      
      if(time / 10000 <= 0) {
         System.err.println("SELECT: " + time + " ms which is " +(ITERATIONS/time)+" per millisecond");         
      } else {
         System.err.println("SELECT: " + time + " ms which is " +(ITERATIONS/(time/1000))+" per second");
      }
   }
   
   public void testOrderBy() throws Exception {
      ChangeDistributor distributor = new ChangeDistributor();
      Catalog catalog = new Catalog(distributor, "test");
      LocalDatabase store = new LocalDatabase(catalog, "test");
      AttributeTableBuilder builder = new AttributeTableBuilder(store);      
      TableBinder binder = builder.createTable("example", ExampleObject.class, "key");
      
      assertNull(catalog.findTable("example"));
      
      binder.create().execute();
      
      assertNotNull(catalog.findTable("example"));      
      
      Random random = new SecureRandom();
      long start = System.currentTimeMillis();
      
      for(int i = 0; i < ITERATIONS; i++) {
         int key = random.nextInt(50000);
         int name = random.nextInt(100000);
         
         binder.insert().execute(new ExampleObject("key-"+key, "name-"+name, "address-"+i, i));
         
         if(i % 10000 == 0){
            System.err.println(i + " at " + (System.currentTimeMillis()-start));
         }
      }
      long end = System.currentTimeMillis();
      long time = end - start;
      
      System.err.println("INSERT:" + time + " ms which is " +(ITERATIONS/(time/1000))+" per second");
      
      start = System.currentTimeMillis();
      
      for(int i = 0; i < 200; i++) {
         long begin = System.currentTimeMillis();
         List<ExampleObject> results = binder.select()
            .where("key > 'key-400'")
            .orderBy("name desc")
            .execute()
            .fetchAll();
         
         System.err.println("Took " + (System.currentTimeMillis() - begin) + " to do " + ITERATIONS + " rows");
         
         List<String> sequence = new ArrayList<String>();
         String prev = null;
         
         for(int j = 0; j < 100; j++) {
            ExampleObject object = results.get(j);
            
            if(prev != null) {
               int value = prev.compareTo(object.name);
               
               if(value < 0) {
                  throw new Exception("Order is wrong as prev is '" + prev + "' and next is '" + object.name + "'");
               }
            }            
            sequence.add(object.name);
            prev = object.name;
         }
         System.err.println(i + ": "+ sequence);
      }
      end = System.currentTimeMillis();
      time = end - start;
   }

}
