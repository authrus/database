package com.authrus.database.engine.io;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.TestCase;

import com.authrus.database.common.io.OutputStreamWriter;

public class DataBlockChangeReaderTest extends TestCase {
   
   private static class DataBlockChangeListener extends Thread {
      
      private final DataRecordFilter source;
      private final List<String> lines;
      private final AtomicBoolean alive;
      
      public DataBlockChangeListener(DataRecordFilter source) {
         this.lines = new ArrayList<String>();
         this.alive = new AtomicBoolean(true);
         this.source = source;
      }
      
      public List<String> lines() {
         return lines;
      }
      
      public void kill(){
         alive.set(false);
      }
      
      public void run() {
         try {
            System.err.println("STARTED....");
            
            while(alive.get()) {
               DataRecord line = source.read();
               if(line == null) {
                  break;
               }
               System.err.println("Reading " + line);
               DataRecordReader reader = line.getReader();
               int count = reader.readInt();
               
               for(int i = 0; i < count;i++) {
                  String text = reader.readString();
                  lines.add(text);
               }
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

   public void testBlockChangeReader() throws Exception {
      File tempDir = new File(System.getProperty("java.io.tmpdir"), "testBlockChangeReader");
      
      if(!tempDir.exists()) {
         tempDir.mkdirs();
      }
      purge(tempDir);
      
      String dir = tempDir.getAbsolutePath();
      FileSeeker filter = new TimeFileSeeker();
      FileBlockConsumer consumer = new FileBlockConsumer(filter, dir);
      DataRecordConsumer recordConsumer = new DataRecordConsumer(consumer);
      DataRecordFilter reader = new DataRecordFilter(recordConsumer);
      DataBlockChangeListener listener = new DataBlockChangeListener(reader);
      FilePathBuilder builder = new FilePathBuilder(dir, "line_test");
      List<String> lines = new ArrayList<String>();
      
      consumer.start();
      listener.start();
      
      for(int i = 0; i < 100; i++) {
         FilePath path = builder.createFile();
         File file = path.getFile();
         System.err.println("CREATE: " + file);
         FileOutputStream out = new FileOutputStream(file);
         OutputStreamRecordProducer producer = new OutputStreamRecordProducer(out);
         DataRecordOutputStream stream = new DataRecordOutputStream(producer);        
         ByteArrayOutputStream buffer = new ByteArrayOutputStream();
         OutputStreamWriter encoder = new OutputStreamWriter(buffer);
         DataRecordWriter writer = new DataRecordWriter(encoder);
         
         try {
            writer.writeInt(10);
            for(int j = 0; j < 10; j++) {
               String line = "i=" + i + " j=" + j;

               writer.writeString(line);
               lines.add(line);
            }
            byte[] data = buffer.toByteArray();
            stream.write(data);
            stream.flush();
         } finally {
            out.close();
         }
         Thread.sleep(2000);
         
         List<String> consumedLines = listener.lines();
         int expectedSize = lines.size();
         int actualSize = consumedLines.size();
         
         assertEquals("Expected " + expectedSize + " but was " + actualSize + " " + consumedLines, actualSize, expectedSize);
         
         for(int j = 0; j < actualSize; j++) {
            String expectedLine = lines.get(j);
            String actualLine = consumedLines.get(j);
            
            assertEquals("Expected " + expectedLine + " but was " + actualLine, actualLine, expectedLine);
            System.err.println("LINES MATCHED!!!!!! [" +expectedLine + "]");
         }
      }
   }
}
