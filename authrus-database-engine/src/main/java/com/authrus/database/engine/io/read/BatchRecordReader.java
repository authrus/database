package com.authrus.database.engine.io.read;

import static com.authrus.database.engine.TransactionType.NONE;
import static com.authrus.database.engine.TransactionType.PERSISTENT;

import java.io.IOException;
import java.util.Collections;

import com.authrus.database.engine.Transaction;
import com.authrus.database.engine.TransactionFilter;
import com.authrus.database.engine.TransactionManager;
import com.authrus.database.engine.io.DataRecordReader;
import com.authrus.database.engine.io.write.ChangeRecordReader;

public class BatchRecordReader implements ChangeRecordReader {

   private final TransactionManager builder;
   private final TransactionFilter filter;  
   private final String table;
   private final boolean restore;
   
   public BatchRecordReader(TransactionFilter filter, String origin, String table) {
      this(filter, origin, table, false);
   }
   
   public BatchRecordReader(TransactionFilter filter, String origin, String table, boolean restore) {
      this.builder = new TransactionManager(Collections.EMPTY_MAP, origin);
      this.restore = restore;
      this.filter = filter;
      this.table = table;      
   } 
   
   @Override
   public ChangeOperation read(DataRecordReader reader) throws IOException {
      String token = reader.readString();
      
      if(token == null) {
         throw new IllegalStateException("Transaction for '" + table + "' has no name");
      }
      Transaction transaction = builder.begin(table, token, restore ? NONE : PERSISTENT);
      Long sequence = transaction.getSequence();
      Long time = transaction.getTime();
      
      if(sequence == null) {
         throw new IllegalStateException("Transaction for '" + table + "' has no sequence");
      }
      if(time == null) {
         throw new IllegalStateException("Transaction for '" + table + "' has no time");
      }      
      return new BeginOperation(filter, transaction);
   }
}
