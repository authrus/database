package com.authrus.database.engine.io.write;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import com.authrus.database.engine.OperationType;
import com.authrus.database.engine.io.write.ChangeRecord;
import com.authrus.database.engine.io.write.DeleteRecordWriter;
import com.authrus.database.engine.io.write.FileLogAppender;

public class FileLogAppenderTest extends TestCase {

   public void testFileLogListener() throws Exception {
      String tempDir = System.getProperty("java.io.tmpdir");
      String name = FileLogAppenderTest.class.getSimpleName()+System.currentTimeMillis();
      FileLogAppender appender = new FileLogAppender(tempDir, "origin", name, 1000, 10);
      List<ChangeRecord> records = new ArrayList<ChangeRecord>();
      
      for(int i = 0; i < 100; i++){
         DeleteRecordWriter writer = new DeleteRecordWriter("origin", "key-"+i);
         ChangeRecord record = new ChangeRecord(writer, OperationType.DELETE, "origin", "table");
         records.add(record);
      }
      for(ChangeRecord record : records) {
         Thread.sleep(10);
         appender.append(record);
      }
   }
}
