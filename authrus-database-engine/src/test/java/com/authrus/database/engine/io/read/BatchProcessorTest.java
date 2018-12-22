package com.authrus.database.engine.io.read;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import junit.framework.TestCase;

import com.authrus.database.engine.OperationType;
import com.authrus.database.engine.io.FilePath;
import com.authrus.database.engine.io.FilePointer;
import com.authrus.database.engine.io.write.BatchRecordProcessor;
import com.authrus.database.engine.io.write.ChangeRecord;
import com.authrus.database.engine.io.write.ChangeRecordBatch;
import com.authrus.database.engine.io.write.ChangeRecordListener;

public class BatchProcessorTest extends TestCase {
   
   private static class MockRecordListener implements ChangeRecordListener {
      BlockingQueue<ChangeRecordBatch> queue = new LinkedBlockingQueue<ChangeRecordBatch>();

      @Override
      public void update(ChangeRecordBatch batch) {
         List<ChangeRecord> records = batch.getRecords();
         for(ChangeRecord record : records) {
            System.err.println("type="+record.getType()+" origin="+record.getOrigin()+" table="+record.getTable());
         }
         System.err.println();
         System.err.println();
         queue.offer(batch);
      }      
   }
   
   public void testMultipleProcessTransaction() throws IOException {
      MockRecordListener listener = new MockRecordListener();
      BatchRecordProcessor processor = new BatchRecordProcessor(listener, null, "owner", "table");
      
      List<ChangeRecord> records1 = Arrays.asList(
            new ChangeRecord(null, OperationType.INSERT, "owner", "table"),
            new ChangeRecord(null, OperationType.UPDATE, "owner", "table")
         );
      List<ChangeRecord> records2 = Arrays.asList(
            new ChangeRecord(null, OperationType.DELETE, "owner", "table"),
            new ChangeRecord(null, OperationType.BEGIN, "tom", "table"),
            new ChangeRecord(null, OperationType.INSERT, "tom", "table")
         );
      List<ChangeRecord> records3 = Arrays.asList(
            new ChangeRecord(null, OperationType.COMMIT, "tom", "table"),
            new ChangeRecord(null, OperationType.BEGIN, "tom", "table"),
            new ChangeRecord(null, OperationType.INSERT, "tom", "table"),
            new ChangeRecord(null, OperationType.INSERT, "tom", "table"),            
            new ChangeRecord(null, OperationType.COMMIT, "tom", "table"),
            new ChangeRecord(null, OperationType.UPDATE, "owner", "table")
         );      
      assertTrue(processor.process(records1));
      
      ChangeRecordBatch record = listener.queue.poll();
      
      assertNotNull(record);
      assertEquals(record.getOrigin(), "owner");
      assertEquals(record.getTable(), "table");
      assertEquals(record.getRecords().size(), 4);
      assertEquals(record.getRecords().get(0).getType(), OperationType.BATCH);       
      assertEquals(record.getRecords().get(1).getType(), OperationType.INSERT);
      assertEquals(record.getRecords().get(2).getType(), OperationType.UPDATE);
      assertEquals(record.getRecords().get(3).getType(), OperationType.COMMIT);
      
      record = listener.queue.poll(); // nothing there as we are still waiting      
      assertNull(record);
      
      assertFalse(processor.process(records2)); // still holding stuff
      record = listener.queue.poll();
      
      assertNotNull(record);
      assertEquals(record.getOrigin(), "owner");
      assertEquals(record.getTable(), "table");
      assertEquals(record.getRecords().size(), 3);
      assertEquals(record.getRecords().get(0).getType(), OperationType.BATCH);      
      assertEquals(record.getRecords().get(1).getType(), OperationType.DELETE);
      assertEquals(record.getRecords().get(2).getType(), OperationType.COMMIT);
      
      record = listener.queue.poll(); // nothing there as we are still waiting      
      assertNull(record);
      
      assertTrue(processor.process(records3));  
      record = listener.queue.poll();
      
      assertNotNull(record);
      assertEquals(record.getOrigin(), "tom");
      assertEquals(record.getTable(), "table");
      assertEquals(record.getRecords().size(), 3);
      assertEquals(record.getRecords().get(0).getType(), OperationType.BEGIN);      
      assertEquals(record.getRecords().get(1).getType(), OperationType.INSERT);
      assertEquals(record.getRecords().get(2).getType(), OperationType.COMMIT);  
      
      record = listener.queue.poll();
      
      assertNotNull(record);
      assertEquals(record.getOrigin(), "tom");
      assertEquals(record.getTable(), "table");
      assertEquals(record.getRecords().size(), 4);
      assertEquals(record.getRecords().get(0).getType(), OperationType.BEGIN);      
      assertEquals(record.getRecords().get(1).getType(), OperationType.INSERT);
      assertEquals(record.getRecords().get(2).getType(), OperationType.INSERT);      
      assertEquals(record.getRecords().get(3).getType(), OperationType.COMMIT); 
      
      record = listener.queue.poll();
      
      assertNotNull(record);
      assertEquals(record.getOrigin(), "owner");
      assertEquals(record.getTable(), "table");
      assertEquals(record.getRecords().size(), 3);
      assertEquals(record.getRecords().get(0).getType(), OperationType.BATCH);      
      assertEquals(record.getRecords().get(1).getType(), OperationType.UPDATE);
      assertEquals(record.getRecords().get(2).getType(), OperationType.COMMIT);
      
      record = listener.queue.poll(); // nothing there as we are still waiting      
      assertNull(record);           
   }
   
   
   public void testMixedOriginBatchAndTransaction() throws IOException {
      MockRecordListener listener = new MockRecordListener();
      BatchRecordProcessor processor = new BatchRecordProcessor(listener, null, "owner", "table");
      List<ChangeRecord> records = Arrays.asList(
         new ChangeRecord(null, OperationType.INSERT, "owner", "table"),
         new ChangeRecord(null, OperationType.UPDATE, "owner", "table"),
         new ChangeRecord(null, OperationType.DELETE, "owner", "table"),
         new ChangeRecord(null, OperationType.BEGIN, "tom", "table"),
         new ChangeRecord(null, OperationType.INSERT, "tom", "table"),
         new ChangeRecord(null, OperationType.COMMIT, "tom", "table"),
         new ChangeRecord(null, OperationType.DELETE, "owner", "table")
      );
      assertTrue(processor.process(records));
      
      ChangeRecordBatch record = listener.queue.poll();
      
      assertNotNull(record);
      assertEquals(record.getOrigin(), "owner");
      assertEquals(record.getTable(), "table");
      assertEquals(record.getRecords().size(), 5);
      assertEquals(record.getRecords().get(0).getType(), OperationType.BATCH);       
      assertEquals(record.getRecords().get(1).getType(), OperationType.INSERT);
      assertEquals(record.getRecords().get(2).getType(), OperationType.UPDATE);
      assertEquals(record.getRecords().get(3).getType(), OperationType.DELETE);
      assertEquals(record.getRecords().get(4).getType(), OperationType.COMMIT);
      
      record = listener.queue.poll();
      
      assertNotNull(record);
      assertEquals(record.getOrigin(), "tom");
      assertEquals(record.getTable(), "table");
      assertEquals(record.getRecords().size(), 3);
      assertEquals(record.getRecords().get(0).getType(), OperationType.BEGIN);      
      assertEquals(record.getRecords().get(1).getType(), OperationType.INSERT);
      assertEquals(record.getRecords().get(2).getType(), OperationType.COMMIT);
      
      record = listener.queue.poll();
      
      assertNotNull(record);
      assertEquals(record.getOrigin(), "owner");
      assertEquals(record.getTable(), "table");
      assertEquals(record.getRecords().size(), 3);
      assertEquals(record.getRecords().get(0).getType(), OperationType.BATCH);      
      assertEquals(record.getRecords().get(1).getType(), OperationType.DELETE);
      assertEquals(record.getRecords().get(2).getType(), OperationType.COMMIT);      
      
   }
   
   public void testMixedBatchAndTransaction() throws IOException {
      MockRecordListener listener = new MockRecordListener();
      BatchRecordProcessor processor = new BatchRecordProcessor(listener, null, "owner", "table");
      List<ChangeRecord> records = Arrays.asList(
         new ChangeRecord(null, OperationType.INSERT, "owner", "table"),
         new ChangeRecord(null, OperationType.UPDATE, "owner", "table"),
         new ChangeRecord(null, OperationType.DELETE, "owner", "table"),
         new ChangeRecord(null, OperationType.BEGIN, "owner", "table"),
         new ChangeRecord(null, OperationType.INSERT, "owner", "table"),
         new ChangeRecord(null, OperationType.COMMIT, "owner", "table")         
      );
      assertTrue(processor.process(records));
      
      ChangeRecordBatch record = listener.queue.poll();
      
      assertNotNull(record);
      assertEquals(record.getOrigin(), "owner");
      assertEquals(record.getTable(), "table");
      assertEquals(record.getRecords().size(), 5);
      assertEquals(record.getRecords().get(0).getType(), OperationType.BATCH);       
      assertEquals(record.getRecords().get(1).getType(), OperationType.INSERT);
      assertEquals(record.getRecords().get(2).getType(), OperationType.UPDATE);
      assertEquals(record.getRecords().get(3).getType(), OperationType.DELETE);
      assertEquals(record.getRecords().get(4).getType(), OperationType.COMMIT);
      
      record = listener.queue.poll();
      
      assertNotNull(record);
      assertEquals(record.getOrigin(), "owner");
      assertEquals(record.getTable(), "table");
      assertEquals(record.getRecords().size(), 3);
      assertEquals(record.getRecords().get(0).getType(), OperationType.BEGIN);      
      assertEquals(record.getRecords().get(1).getType(), OperationType.INSERT);
      assertEquals(record.getRecords().get(2).getType(), OperationType.COMMIT);
      
   }
   
   public void testSimpleTransaction() throws IOException {
      MockRecordListener listener = new MockRecordListener();
      BatchRecordProcessor processor = new BatchRecordProcessor(listener, null, "owner", "table");
      List<ChangeRecord> records = Arrays.asList(
         new ChangeRecord(null, OperationType.BEGIN, "owner", "table"),
         new ChangeRecord(null, OperationType.INSERT, "owner", "table"),
         new ChangeRecord(null, OperationType.UPDATE, "owner", "table"),
         new ChangeRecord(null, OperationType.DELETE, "owner", "table"),
         new ChangeRecord(null, OperationType.INSERT, "owner", "table"),
         new ChangeRecord(null, OperationType.COMMIT, "owner", "table")         
      );
      assertTrue(processor.process(records));
      assertNotNull(listener.queue.peek());
      
      ChangeRecordBatch record = listener.queue.poll();
      
      assertEquals(record.getOrigin(), "owner");
      assertEquals(record.getTable(), "table");
      assertEquals(record.getRecords().size(), 6);
      assertEquals(record.getRecords().get(0).getType(), OperationType.BEGIN);
      assertEquals(record.getRecords().get(1).getType(), OperationType.INSERT);
      assertEquals(record.getRecords().get(2).getType(), OperationType.UPDATE);
      assertEquals(record.getRecords().get(3).getType(), OperationType.DELETE);
      assertEquals(record.getRecords().get(4).getType(), OperationType.INSERT);
      assertEquals(record.getRecords().get(5).getType(), OperationType.COMMIT);
      
   }
   
   public void testSimpleBatch() throws IOException {
      MockRecordListener listener = new MockRecordListener();
      BatchRecordProcessor processor = new BatchRecordProcessor(listener, null, "owner", "table");
      List<ChangeRecord> records = Arrays.asList(
         new ChangeRecord(null, OperationType.INSERT, "owner", "table"),
         new ChangeRecord(null, OperationType.INSERT, "owner", "table"),
         new ChangeRecord(null, OperationType.INSERT, "owner", "table"),
         new ChangeRecord(null, OperationType.INSERT, "owner", "table")
      );
      assertTrue(processor.process(records));
      assertNotNull(listener.queue.peek());
      
      ChangeRecordBatch record = listener.queue.poll();
      
      assertEquals(record.getOrigin(), "owner");
      assertEquals(record.getTable(), "table");
      assertEquals(record.getRecords().size(), 6);
      assertEquals(record.getRecords().get(0).getType(), OperationType.BATCH);
      assertEquals(record.getRecords().get(1).getType(), OperationType.INSERT);
      assertEquals(record.getRecords().get(2).getType(), OperationType.INSERT);
      assertEquals(record.getRecords().get(3).getType(), OperationType.INSERT);
      assertEquals(record.getRecords().get(4).getType(), OperationType.INSERT);
      assertEquals(record.getRecords().get(5).getType(), OperationType.COMMIT);
      
   }

}
