package com.authrus.database.engine.io.write;

import static com.authrus.database.engine.OperationType.BATCH;

import java.io.IOException;

import com.authrus.database.engine.io.DataRecordCounter;
import com.authrus.database.engine.io.DataRecordWriter;

public class BatchRecordWriter implements ChangeRecordWriter {

   private final StringBuilder builder;
   private final String owner;

   public BatchRecordWriter(String owner) {
      this.builder = new StringBuilder();
      this.owner = owner;
   }

   @Override
   public void write(DataRecordWriter writer, DataRecordCounter counter) throws IOException {
      String origin = counter.getOrigin();
      long sequence = counter.getNext(); // increment sequence
      long time = counter.getTime();
      
      if(!owner.equals(origin)) {
         throw new IOException("Batch origin is '" + origin +"' but should be '" + owner +"'");
      }
      builder.setLength(0);
      builder.append(sequence); // just use sequence for a batch
      builder.append('@');
      builder.append(origin);
      builder.append('.');
      builder.append(time);
      builder.append('.');
      builder.append(sequence);
      
      String name = builder.toString();
      
      writer.writeChar(BATCH.code);
      writer.writeString(origin);
      writer.writeString(name);
      
   }      
}