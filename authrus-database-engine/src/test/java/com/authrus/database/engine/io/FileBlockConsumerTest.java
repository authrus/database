package com.authrus.database.engine.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.security.SecureRandom;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;

import com.authrus.database.engine.io.FileBlockConsumer;
import com.authrus.database.engine.io.FileSeeker;
import com.authrus.database.engine.io.TimeFileSeeker;

import junit.framework.TestCase;

public class FileBlockConsumerTest extends TestCase {
   
   private static class FileBlockListener extends Thread {
      
      private final InputStream source;
      private final AtomicBoolean alive;
      private final CRC32 checker;
      
      public FileBlockListener(InputStream source) {
         this.checker = new CRC32();
         this.alive = new AtomicBoolean(true);
         this.source = source;
      }
      
      public long checkSum(){
         return checker.getValue();
      }
      
      public void kill(){
         alive.set(false);
      }
      
      public void run() {
         try {
            System.err.println("STARTED....");
            Random random = new SecureRandom();
            byte[] data = new byte[1024];
            
            while(alive.get()) {
               int size = random.nextInt(1000) + 1;
               int count = source.read(data, 0, size);
               
               checker.update(data, 0, count);
               System.err.write(data, 0, count);
            }
            System.err.println("ENDED...");
         } catch(Exception e) {
            e.printStackTrace();
         }
      }
   }
   
   
   public static void purge(File file){
      if(file.isDirectory()) {
         File[] tempFiles = file.listFiles();
         
         for(File tempFile : tempFiles) {
            if(tempFile.isFile()) {               
               tempFile.delete();               
            }else {
              purge(tempFile);
              tempFile.delete();               
            }
         }
      }      
   }
   
   public void testBlockOrder() throws Exception {
      File tempDir = new File(System.getProperty("java.io.tmpdir"), "testBlockOrder");
      
      if(!tempDir.exists()) {
         tempDir.mkdirs();
      }
      purge(tempDir);
      
      String dir = tempDir.getAbsolutePath();
      FileSeeker filter = new TimeFileSeeker();
      FileBlockConsumer consumer = new FileBlockConsumer(filter, dir);
      DataBlockInputStream stream = new DataBlockInputStream(consumer);
      FileBlockListener listener = new FileBlockListener(stream);
      DateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS");
      Random random = new SecureRandom();
      CRC32 expect = new CRC32();
      
      consumer.start();
      listener.start();
      
      for(int i = 0; i < 100; i++) {
         Date date = new Date();
         File file = new File(tempDir, "blah." + format.format(date));
         System.err.println("CREATE: " + file);
         FileOutputStream out = new FileOutputStream(file);
         
         try {
            for(int j = 0; j < 10; j++) {
               int number = random.nextInt(1000000);
               String token = String.valueOf(number);
               byte[] data = token.getBytes();
               
               out.write(data);
               expect.update(data, 0, data.length);
            }
         } finally {
            out.close();
         }
         Thread.sleep(2000);
         
         long expectSum = expect.getValue();
         long actualSum = listener.checkSum();
         
         assertEquals("Expected " + expectSum + " but was " + actualSum, actualSum, expectSum);
         System.err.println("CHECKSUMS MATCH!!!!! " + expectSum);
      }
      
   }

}
