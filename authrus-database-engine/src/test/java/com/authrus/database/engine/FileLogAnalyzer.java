package com.authrus.database.engine;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

import com.authrus.database.Schema;
import com.authrus.database.engine.Transaction;
import com.authrus.database.engine.TransactionFilter;
import com.authrus.database.engine.io.DataRecord;
import com.authrus.database.engine.io.DataRecordConsumer;
import com.authrus.database.engine.io.DataRecordIterator;
import com.authrus.database.engine.io.FileBlockConsumer;
import com.authrus.database.engine.io.FileSeeker;
import com.authrus.database.engine.io.TimeFileSeeker;
import com.authrus.database.engine.io.read.ChangeAssembler;
import com.authrus.database.engine.io.read.ChangeDispatcher;
import com.authrus.database.engine.io.read.ChangeOperation;
import com.authrus.database.engine.io.read.ChangeScheduler;
import com.authrus.database.engine.io.read.ChangeSet;

public class FileLogAnalyzer {

   public static void main(String[] list) throws Exception {
      String directory = "C:\\Work\\development\\bitbucket\\database\\zuooh-shared-database-terminal\\database\\slave";

      if (list.length == 1) {
         directory = list[0];
      }
      FileSeeker filter = new TimeFileSeeker();
      FileBlockConsumer consumer = new FileBlockConsumer(filter, directory);
      DataRecordConsumer source = new DataRecordConsumer(consumer);
      DataRecordIterator iterator = new DataRecordIterator(source, "profile");
      ChangeAnalyzer analyzer = new ChangeAnalyzer();
      ChangeDispatcher dispatcher = new ChangeDispatcher(analyzer, analyzer);

      consumer.start();

      while (iterator.hasNext()) {
         DataRecord record = iterator.next();
         dispatcher.dispatch(record);
      }
   }

   private static class ChangeAnalyzer implements ChangeAssembler, ChangeScheduler, TransactionFilter {

      private int transactions;
      private int changes;

      @Override
      public void onBegin(String user, String name, Transaction transaction) {
         System.err.println("origin=[" + user + "] table=[" + name + "] transactions=["+transactions+"] changes=["+changes+"] type=BEGIN");
         changes++;
      }

      @Override
      public void onCreate(String user, String name, Schema schema) {
         System.err.println("origin=[" + user + "] table=[" + name + "] transactions=["+transactions+"] changes=["+changes+"]  type=CREATE");
         changes++;
      }

      @Override
      public void onInsert(String user, String name, ChangeSet change) {
         System.err.println("origin=[" + user + "] table=[" + name + "] transactions=["+transactions+"] changes=["+changes+"]  type=INSERT");
         changes++;
      }

      @Override
      public void onUpdate(String user, String name, ChangeSet change) {
         System.err.println("origin=[" + user + "] table=[" + name + "] transactions=["+transactions+"] changes=["+changes+"]  type=UPDATE");
         changes++;
      }

      @Override
      public void onDelete(String user, String name, String key) {
         System.err.println("origin=[" + user + "] table=[" + name + "] transactions=["+transactions+"] changes=["+changes+"]  type=DELETE");
         changes++;
      }

      @Override
      public void onIndex(String user, String name, String column) {
         System.err.println("origin=[" + user + "] table=[" + name + "] transactions=["+transactions+"] changes=["+changes+"]  type=INDEX");
         changes++;
      }

      @Override
      public void onCommit(String user, String name) {
         System.err.println("origin=[" + user + "] table=[" + name + "] transactions=["+transactions+"] changes=["+changes+"]  type=COMMIT");
         changes++;
      }

      @Override
      public void onDrop(String user, String name) {
         System.err.println("origin=[" + user + "] table=[" + name + "] transactions=["+transactions+"] changes=["+changes+"]  type=DROP");
         changes++;
      }
      
      @Override
      public void onRollback(String user, String name) {
         System.err.println("origin=[" + user + "] table=[" + name + "] transactions=["+transactions+"] changes=["+changes+"]  type=ROLLBACK");
         changes++;
      }      

      @Override
      public void schedule(ChangeOperation operation) {
         operation.execute(this);
      }

      @Override
      public boolean accept(Transaction transaction) {
         transactions++;
         return true;
      }

   }
}
