package com.authrus.database.engine;

import java.io.File;
import java.io.Serializable;
import java.text.DecimalFormat;

import junit.framework.TestCase;

import com.authrus.database.bind.TableBinder;
import com.authrus.database.bind.table.attribute.AttributeTableBuilder;
import com.authrus.database.engine.io.write.ChangeLogPersister;
import com.authrus.database.engine.io.write.FileLog;

public class InsertPerformanceTest extends TestCase {
   
   private static final int ITERATIONS = 1000000;
   
   public static enum AccountType {
      ANONYMOUS, 
      PREMIUM, 
      ACTIVE, 
      INACTIVE;
   }
   
   public static class Account implements Serializable {

      private static final long serialVersionUID = 1L;

      private AccountType type; // 16 bytes
      private String key; // 20 bytes
      private String name; // 20 bytes
      private String mail; // 20 bytes
      private String password; // 30 bytes
      private int rank; // 4 bytes
      private int percentile; // 4 bytes
      private double rating; // 8 bytes
      private double deviation; // 8 bytes
      private double volatility; // 8 bytes
      private int blackGames; // 4 bytes
      private int whiteGames; // 4 bytes
      private long lastSeenOnline; // 8 bytes // 170 bytes!!!

      public Account(AccountType type, String key, String name, String mail, String password) {
         this.key = key.toLowerCase();
         this.password = password;
         this.type = type;
         this.name = name;
         this.mail = mail;
      }

      public AccountType getType() {
         return type;
      }

      public void setType(AccountType type) {
         this.type = type;
      }

      public String getMail() {
         return mail;
      }

      public void setMail(String mail) {
         this.mail = mail;
      }

      public String getName() {
         return name;
      }

      public void setName(String name) {
         this.name = name;
      }

      public String getPassword() {
         return password;
      }

      public void setPassword(String password) {
         this.password = password;
      }

      public String getKey() {
         return key;
      }

      public void setKey(String userId) {
         this.key = userId;
      }

      public int getRank() {
         return rank;
      }

      public void setRank(int rank) {
         this.rank = rank;
      }

      public int getPercentile() {
         return percentile;
      }

      public void setPercentile(int percentile) {
         this.percentile = percentile;
      }

      public double getRating() {
         return rating;
      }

      public void setRating(double rating) {
         this.rating = rating;
      }

      public double getVolatility() {
         return volatility;
      }

      public void setVolatility(double volatility) {
         this.volatility = volatility;
      }

      public double getDeviation() {
         return deviation;
      }

      public void setDeviation(double deviation) {
         this.deviation = deviation;
      }

      public int getBlackGames() {
         return blackGames;
      }

      public void setBlackGames(int blackGames) {
         this.blackGames = blackGames;
      }

      public int getWhiteGames() {
         return whiteGames;
      }

      public void setWhiteGames(int whiteGames) {
         this.whiteGames = whiteGames;
      }

      public long getLastSeenOnline() {
         return lastSeenOnline;
      }

      public void setLastSeenOnline(long lastSeenOnline) {
         this.lastSeenOnline = lastSeenOnline;
      }

      @Override
      public String toString() {
         return String.format("name=%s id=%s", name, key);
      }
   }
   
   public void testWithoutPersistence() throws Exception {
      ChangeListener listener = new ChangeDistributor();
      testInsertPerformance("NO PERSISTENCE", listener);
   } 
   
   public void testWithPersistence() throws Exception {
      String directory = System.getProperty("java.io.tmpdir") + "\\testInsertPerformance"; 
      FileLog log = new FileLog(directory, "test", 1000000, 1000);
      File file = new File(directory);
      System.err.println("Persisting to " + file.getCanonicalPath());
      ChangeListener listener = new ChangeLogPersister(log);
      
      if(file.exists()) {
         clearDirectory(file);
      } else {
         file.mkdirs();
      }
      log.start();      
      testInsertPerformance("PERSISTENCE", listener);
   }  
   
   public void testInsertPerformance(String tag, ChangeListener listener) throws Exception {
      DecimalFormat format = new DecimalFormat("###,###,###,###");
      Catalog catalog = new Catalog(listener, "test");      
      LocalDatabase store = new LocalDatabase(catalog, "test");
      AttributeTableBuilder builder = new AttributeTableBuilder(store);      
      TableBinder<Account> binder = builder.createTable("account", Account.class, "key");
      
      assertNull(catalog.findTable("account"));      
      binder.create().execute();      
      assertNotNull(catalog.findTable("account"));
      
      Account[] record = new Account[ITERATIONS];
      
      for(int i = 0; i < ITERATIONS; i++) {
         record[i] = new Account(AccountType.PREMIUM, "key-" + i, "name-" + i, "mail-" + i + "@address.com", "password12");         
      }
      System.gc();
      
      long start = System.currentTimeMillis();
      
      for(int i = 0; i < ITERATIONS; i++) {
         binder.insert().execute(record[i]);
         
         if(i % 100000 == 0) {
            System.err.println(i);
         }
      }
      long end = System.currentTimeMillis();
      long duration = Math.max(end - start, 1);
      
      long insertsPerMillis = (ITERATIONS/duration);
      long insertsPerSecond = insertsPerMillis * 1000;
      
      System.err.println(tag+": durationMillis=[" + duration + "] insertCount=["+ITERATIONS+"] throughputPerSec=[" + format.format(insertsPerSecond) +"]");
      System.err.flush();
      
      Thread.sleep(5000);
   }   
   
   private static void clearDirectory(File file){
      if(file.isDirectory()) {
         File[] tempFiles = file.listFiles();
         
         for(File tempFile : tempFiles) {
            if(tempFile.isFile()) {               
               tempFile.delete();               
            }else {
              clearDirectory(tempFile);
              tempFile.delete();               
            }
         }
      }      
   }   
   
   public static void main(String[] list) throws Exception {
      new InsertPerformanceTest().setUp();      
      new InsertPerformanceTest().testWithoutPersistence();
      new InsertPerformanceTest().testWithPersistence();        
   }
}
