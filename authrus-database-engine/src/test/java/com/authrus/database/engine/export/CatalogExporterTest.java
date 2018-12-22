package com.authrus.database.engine.export;

import java.io.File;
import java.io.Serializable;
import java.text.DecimalFormat;

import junit.framework.TestCase;

import com.authrus.database.bind.TableBinder;
import com.authrus.database.bind.table.attribute.AttributeTableBuilder;
import com.authrus.database.bind.table.statement.InsertStatement;
import com.authrus.database.engine.Catalog;
import com.authrus.database.engine.ChangeListener;
import com.authrus.database.engine.LocalDatabase;
import com.authrus.database.engine.export.CatalogExporter;
import com.authrus.database.engine.export.SpreadSheetEscaper;
import com.authrus.database.engine.export.ValueEscaper;
import com.authrus.database.engine.io.write.ChangeLogPersister;
import com.authrus.database.engine.io.write.FileLog;

public class CatalogExporterTest extends TestCase {
   
   private static class ChatMessage implements Serializable {
      private String fromId;
      private String toId;
      private String chat;
      private long time;
      private int id;
      
      public ChatMessage(String fromId, String toId, String chat, long time, int id){
         this.fromId = fromId;
         this.toId = toId;
         this.chat = chat;
         this.time = time;
         this.id = id;
      }
   }

   public void testExport() throws Exception {
      String directory = System.getProperty("java.io.tmpdir") + "\\testCatalogExporter"; 
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
      DecimalFormat format = new DecimalFormat("###,###,###,###");
      Catalog catalog = new Catalog(listener, "test");      
      LocalDatabase store = new LocalDatabase(catalog, "test");
      AttributeTableBuilder builder = new AttributeTableBuilder(store);      
      TableBinder<ChatMessage> binder = builder.createTable("chat", ChatMessage.class, "id");
      
      assertNull(catalog.findTable("chat"));      
      binder.create().execute();      
      assertNotNull(catalog.findTable("chat"));
      
      ChatMessage[] records = new ChatMessage[100000];
      
      for(int i = 0; i < records.length; i++) {
         records[i] = new ChatMessage("user-"+i, "to-"+i, "hello user-"+i+", this is to-"+i+" speaking, how are you?\ntalk later,\nto-"+i, System.currentTimeMillis(), i);      
      }
      System.gc();
      
      InsertStatement<ChatMessage> statement = binder.insert();
      String expression = statement.compile();
      
      System.err.println(expression);
      
      for(int i = 0; i < records.length; i++) {
         statement.execute(records[i]);
         
         if(i % 100000 == 0) {
            System.err.println(i);
         }
      }
      ValueEscaper escaper = new SpreadSheetEscaper();
      CatalogExporter exporter = new CatalogExporter(escaper);
      
      long start = System.currentTimeMillis();
      
      exporter.export(catalog, file);
      
      long end = System.currentTimeMillis();
      long duration = Math.max(end - start, 1);
      
      long insertsPerMillis = (records.length/duration);
      long insertsPerSecond = insertsPerMillis * 1000;
      
      System.err.println("EXPORT FINISHED: durationMillis=[" + duration + "] insertCount=["+records.length+"] throughputPerSec=[" + format.format(insertsPerSecond) +"]");
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
}
