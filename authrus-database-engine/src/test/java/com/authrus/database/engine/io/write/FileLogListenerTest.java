package com.authrus.database.engine.io.write;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import com.authrus.database.engine.OperationType;
import com.authrus.database.engine.io.FilePointer;
import com.authrus.database.engine.io.write.ChangeRecord;
import com.authrus.database.engine.io.write.ChangeRecordBatch;
import com.authrus.database.engine.io.write.DeleteRecordWriter;
import com.authrus.database.engine.io.write.FileLogListener;

public class FileLogListenerTest extends TestCase {   
   
   public void testFileLogListener() throws Exception {
      String tempDir = System.getProperty("java.io.tmpdir");
      String name = FileLogListenerTest.class.getSimpleName()+System.currentTimeMillis();
      FilePointer pointer = new FilePointer(tempDir, name);
      FileLogListener listener = new FileLogListener(pointer, "origin", 10);
      List<ChangeRecord> records = new ArrayList<ChangeRecord>();
      ChangeRecordBatch batch = new ChangeRecordBatch(records, "origin", "table");
      
      for(int i = 0; i < 100; i++){
         DeleteRecordWriter writer = new DeleteRecordWriter("origin", "key-"+i);
         ChangeRecord record = new ChangeRecord(writer, OperationType.DELETE, "origin", "table");
         records.add(record);
      }
      listener.update(batch);
   }

}
