package com.authrus.database.engine.io.write;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Collections;

import junit.framework.TestCase;

import com.authrus.database.common.io.InputStreamReader;
import com.authrus.database.common.io.OutputStreamWriter;
import com.authrus.database.engine.OperationType;
import com.authrus.database.engine.Transaction;
import com.authrus.database.engine.TransactionFilter;
import com.authrus.database.engine.TransactionManager;
import com.authrus.database.engine.io.DataRecordReader;
import com.authrus.database.engine.io.DataRecordWriter;
import com.authrus.database.engine.io.read.BeginRecordReader;
import com.authrus.database.engine.io.read.ChangeOperation;

public class BeginRecordWriterTest extends TestCase {
   
   private static class MockFilter implements TransactionFilter{
      Transaction transaction;

      @Override
      public boolean accept(Transaction transaction) {
         this.transaction = transaction;
         return false;
      }      
   }
   
   public void testBeginRecordWriter() throws Exception {
      TransactionManager builder = new TransactionManager(Collections.EMPTY_MAP, "owner");
      Transaction transaction = builder.begin("table", "test@blah.123.456");
      BeginRecordWriter writer = new BeginRecordWriter("blah", transaction);
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      OutputStreamWriter encoder = new OutputStreamWriter(buffer);
      DataRecordWriter recordWriter = new DataRecordWriter(encoder);
            
      writer.write(recordWriter, null);
      
      byte[] result = buffer.toByteArray();
      ByteArrayInputStream input = new ByteArrayInputStream(result);
      InputStreamReader decoder = new InputStreamReader(input);
      DataRecordReader recordReader = new DataRecordReader(decoder);
      MockFilter filter = new MockFilter();
      BeginRecordReader beginReader = new BeginRecordReader(filter, "owner", "table");
      
      assertEquals(recordReader.readChar(), OperationType.BEGIN.code);
      assertEquals(recordReader.readString(), "blah");
      ChangeOperation operation = beginReader.read(recordReader);
      
      operation.execute(null);
      assertNotNull(filter.transaction);
      assertEquals(filter.transaction.getName(), "test");     
      assertEquals(filter.transaction.getOrigin(), "blah");      
      assertEquals(filter.transaction.getTime(), new Long(123));
      assertEquals(filter.transaction.getSequence(), new Long(456));
      assertNotNull(filter.transaction);
      
   }
   

}
