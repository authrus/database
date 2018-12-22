package com.authrus.database.engine.io;

import java.io.File;
import java.util.List;

import com.authrus.database.engine.io.FilePath;
import com.authrus.database.engine.io.FilePathScanner;
import com.authrus.database.engine.io.FilePointer;
import com.authrus.database.engine.io.FileRecordProducer;

import junit.framework.TestCase;

public class FileRecordProducerTest extends TestCase {   
   
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
   
   public void testFileRecordProducer() throws Exception {
      File tempDir = new File(System.getProperty("java.io.tmpdir"), "testFileRecordProducer");
      
      if(!tempDir.exists()) {
         tempDir.mkdirs();
      }
      purge(tempDir);
      
      String dir = tempDir.getAbsolutePath();
      FilePointer pointer = new FilePointer(dir, "record");
      FileRecordProducer producer = new FileRecordProducer(pointer);
      FilePathScanner scanner = new FilePathScanner(dir);
      List<FilePath> paths = scanner.listFiles();
      
      assertTrue(paths.isEmpty());
      producer.produce("record-1".getBytes("UTF-8"));
      producer.produce("record-2".getBytes("UTF-8"));
      producer.produce("record-3".getBytes("UTF-8"));
      
      paths = scanner.listFiles();
      
      assertEquals(paths.size(), 1);
      Thread.sleep(1000);
      
      assertNotNull(pointer.next());
      producer.produce("record-1".getBytes("UTF-8"));
      producer.produce("record-2".getBytes("UTF-8"));
      producer.produce("record-3".getBytes("UTF-8"));
      
      paths = scanner.listFiles();
      
      assertEquals(paths.size(), 2);
   }

}
