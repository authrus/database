package com.authrus.database.engine.io.replicate;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.Socket;
import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;

import junit.framework.TestCase;

import com.authrus.database.engine.io.DataBlockInputStream;
import com.authrus.database.engine.io.FilePath;
import com.authrus.database.engine.io.FilePathBuilder;
import com.authrus.database.engine.io.replicate.Position;
import com.authrus.database.engine.io.replicate.RemoteBlockConsumer;
import com.authrus.database.engine.io.replicate.RemoteBlockHandler;
import com.authrus.database.engine.io.replicate.RemoteBlockServer;
import com.authrus.database.engine.io.replicate.RemoteConnection;
import com.authrus.database.engine.io.replicate.RemoteSocket;

public class RemoteBlockServerTest extends TestCase {
   
   private static class RemoteBlockClient extends Thread {
      
      private final InputStream source;
      private final AtomicBoolean alive;
      private final CRC32 checker;
      
      public RemoteBlockClient(InputStream source) {
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
   
   public void testSocketBlockOrder() throws Exception {
      File tempDir = new File(System.getProperty("java.io.tmpdir"), "testSocketBlockOrder");
      
      if(!tempDir.exists()) {
         tempDir.mkdirs();
      }
      purge(tempDir);
      
      RemoteBlockHandler handler = new RemoteBlockHandler(tempDir.getCanonicalPath());
      RemoteBlockServer server = new RemoteBlockServer(handler, 4455);
      Random random = new SecureRandom();
      
      server.start();
      
      String dir = tempDir.getAbsolutePath();
      Socket socket = new Socket("localhost", 4455);
      Position position = new Position();
      RemoteConnection connection = new RemoteSocket(socket);
      RemoteBlockConsumer consumer = new RemoteBlockConsumer(connection, position);
      DataBlockInputStream stream = new DataBlockInputStream(consumer);
      RemoteBlockClient client = new RemoteBlockClient(stream);
      FilePathBuilder builder = new FilePathBuilder(dir, "socket_test");
      CRC32 expect = new CRC32();
      
      client.start();
      
      for(int i = 0; i < 100; i++) {
         FilePath path = builder.createFile();
         File file = path.getFile();
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
         Thread.sleep(5000);
         
         long expectSum = expect.getValue();
         long actualSum = client.checkSum();
         
         assertEquals("Expected " + expectSum + " but was " + actualSum, actualSum, expectSum);
         System.err.println("CHECKSUMS MATCH!!!!! " + expectSum);    
      }
   }

}
