package com.authrus.database.engine.text;

import java.io.File;

import junit.framework.TestCase;

import com.authrus.database.Schema;
import com.authrus.database.common.thread.ThreadPool;
import com.authrus.database.engine.Transaction;
import com.authrus.database.engine.TransactionFilter;
import com.authrus.database.engine.io.DataRecordConsumer;
import com.authrus.database.engine.io.DataRecordIterator;
import com.authrus.database.engine.io.FileBlockConsumer;
import com.authrus.database.engine.io.FileSeeker;
import com.authrus.database.engine.io.TimeFileSeeker;
import com.authrus.database.engine.io.read.ChangeAssembler;
import com.authrus.database.engine.io.read.ChangeProcessor;
import com.authrus.database.engine.io.read.ChangeScheduler;
import com.authrus.database.engine.io.read.ChangeSet;
import com.authrus.database.engine.io.read.ThreadPoolScheduler;
import com.authrus.database.engine.io.replicate.Position;
import com.authrus.database.engine.io.replicate.RestoreFilter;

public class TokenReaderPerformanceTest extends TestCase {
   
   private static class DummyAssembler implements ChangeAssembler {
      public void onBegin(String origin, String name, Transaction transaction) {}   
      public void onCreate(String origin, String name, Schema schema) {}
      public void onInsert(String origin, String name, ChangeSet change) {}
      public void onUpdate(String origin, String name, ChangeSet change) {}
      public void onDelete(String origin, String name, String key) {}
      public void onIndex(String origin, String name, String column) {}
      public void onDrop(String origin, String name) {}         
      public void onCommit(String origin, String name) {}
      public void onRollback(String user, String name) {}
   }
/*
   public void testReadOnlySpeed() throws Exception {
      ConsoleAppender consoleAppender = new ConsoleAppender(); // create appender
      PatternLayout logLayout = new PatternLayout("%d [%p|%c|%C{1}] %m%n");
      
      consoleAppender.setLayout(logLayout);
      consoleAppender.setThreshold(Level.DEBUG);
      consoleAppender.activateOptions();
      
      Logger.getRootLogger().addAppender(consoleAppender);
      
      File directory = new File("C:\\Work\\development\\bitbucket\\database\\zuooh-shared-database-terminal\\database");
      FileBlockConsumer consumer = new FileBlockConsumer(directory, "master");
      DataBlockReader stream = new DataBlockReader(consumer);
      DataBlockChangeReader reader = new DataBlockChangeReader(stream);
      long start = System.currentTimeMillis();
      Calendar calendar = Calendar.getInstance();
      consumer.start();
      String line = reader.readNext();
      int count=0;
      while(line!=null){
         TokenReader tokenReader = new TokenReader(calendar, line);
         tokenReader.readDate();
         tokenReader.readSymbol();
         tokenReader.readLong();
         String command = tokenReader.readSymbol();
         
         if(command.equals("insert")) {
            ChangeType.resolveType(command);
            tokenReader.readSymbol();
            tokenReader.readList();
         }
         if(count++ %1000000 == 0){
            System.err.println("Time was "+(System.currentTimeMillis()-start) + " for " + count);
         }
         line = reader.readNext();       
         ///tokenReader.readLong();
         
      }
      System.err.println("Time was "+(System.currentTimeMillis()-start));
   }*/
   
   public String createTempDir(String name) {
      String directory = System.getProperty("java.io.tmpdir"); 
      File newDir = new File(directory, name);
      if(!newDir.exists()) {
         newDir.mkdirs();
      }
      return newDir.getAbsolutePath();      
   }   
   
   public void testSpeed() throws Exception {
      DummyAssembler assembler = new DummyAssembler();
      ThreadPool pool = new ThreadPool(1);
      Position position = new Position();
      TransactionFilter filter = new RestoreFilter(position, "master", 1000000);
      ChangeScheduler executor = new ThreadPoolScheduler(assembler, pool);
      ChangeProcessor processor = new ChangeProcessor(executor, filter);
      FileSeeker fileFilter = new TimeFileSeeker();
      FileBlockConsumer consumer = new FileBlockConsumer(fileFilter, "C:\\Work\\development\\bitbucket\\database\\zuooh-shared-database-terminal\\database\\master");
      DataRecordConsumer stream = new DataRecordConsumer(consumer);
      DataRecordIterator iterator = new DataRecordIterator(stream, "test");
      long start = System.currentTimeMillis();
      
      consumer.start();
      processor.process(iterator);
      System.err.println("Time was "+(System.currentTimeMillis()-start));
   }

}
