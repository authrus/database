package com.authrus.database.engine;

import static com.authrus.database.engine.TransactionType.FULL;

import java.util.Collections;

import com.authrus.database.engine.Transaction;
import com.authrus.database.engine.TransactionManager;

import junit.framework.TestCase;

public class TransactionBuilderTest extends TestCase {

   public void testTransientTransaction() {
      TransactionManager builder = new TransactionManager(Collections.EMPTY_MAP, "owner");      
      Transaction transaction = builder.begin("table", "xx", FULL);
      
      assertEquals(transaction.getName(), "xx");
      assertNull(transaction.getOrigin(), null);
      assertNull(transaction.getSequence());
      assertNull(transaction.getTime());
   }
   
   public void testSimpleTransactionBuilder() {
      TransactionManager builder = new TransactionManager(Collections.EMPTY_MAP, "owner");      
      Transaction transaction = builder.begin("table", "xx");
      
      assertEquals(transaction.getName(), "xx");
      assertEquals(transaction.getOrigin(), "owner");
      assertNull(transaction.getSequence());
      assertNull(transaction.getTime());
   }
   
   public void testTransactionBuilder() {
      TransactionManager builder = new TransactionManager(Collections.EMPTY_MAP, "owner");      
      Transaction transaction = builder.begin("table", "xx@server.123.1");
      
      assertEquals(transaction.getName(), "xx");
      assertEquals(transaction.getOrigin(), "server");
      assertEquals(transaction.getSequence(), new Long(1L));
      assertEquals(transaction.getTime(), new Long(123L));
   }
}
