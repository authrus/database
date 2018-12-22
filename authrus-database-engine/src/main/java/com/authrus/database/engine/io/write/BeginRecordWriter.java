package com.authrus.database.engine.io.write;

import static com.authrus.database.engine.OperationType.BEGIN;

import java.io.IOException;

import com.authrus.database.engine.Transaction;
import com.authrus.database.engine.io.DataRecordCounter;
import com.authrus.database.engine.io.DataRecordWriter;

public class BeginRecordWriter implements ChangeRecordWriter {
   
   private final Transaction transaction;
   private final StringBuilder builder;
   private final String origin;
   
   public BeginRecordWriter(String origin, Transaction transaction) {
      this.builder = new StringBuilder();
      this.transaction = transaction;     
      this.origin = origin;     
   }

   @Override
   public void write(DataRecordWriter writer, DataRecordCounter counter) throws IOException {  
      String name = transaction.getName(); // not important
      Long time = transaction.getTime();
      Long sequence = transaction.getSequence();      
      
      if(time == null || sequence == null) { // local transaction
         String owner = counter.getOrigin();
         
         if(!owner.equals(origin)) {
            throw new IOException("Transaction origin is '" + origin +"' but should be '" + owner +"'");
         }
         time = counter.getTime();
         sequence = counter.getNext();
      }
      builder.setLength(0);
      builder.append(name);
      builder.append('@');
      builder.append(origin);
      builder.append('.');
      builder.append(time);
      builder.append('.');
      builder.append(sequence);
      
      String token = builder.toString();
      
      writer.writeChar(BEGIN.code);
      writer.writeString(origin);
      writer.writeString(token);
   }
}