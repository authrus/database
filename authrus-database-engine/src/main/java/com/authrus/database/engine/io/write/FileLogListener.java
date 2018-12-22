package com.authrus.database.engine.io.write;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import com.authrus.database.common.io.DataWriter;
import com.authrus.database.common.io.OutputStreamWriter;
import com.authrus.database.engine.io.DataRecordOutputStream;
import com.authrus.database.engine.io.DataRecordWriter;
import com.authrus.database.engine.io.FilePointer;
import com.authrus.database.engine.io.FileRecordCounter;
import com.authrus.database.engine.io.FileRecordProducer;

public class FileLogListener implements ChangeRecordListener {
   
   private final FileRecordProducer producer;
   private final FileRecordCounter counter;
   private final OutputStream stream;
   private final int group;

   public FileLogListener(FilePointer pointer, String owner) {
      this(pointer, owner, 1000);
   }
   
   public FileLogListener(FilePointer pointer, String owner, int group) {
      this.counter = new FileRecordCounter(pointer, owner);
      this.producer = new FileRecordProducer(pointer);
      this.stream = new DataRecordOutputStream(producer);
      this.group = group;
   }

   @Override
   public void update(ChangeRecordBatch batch) throws IOException { 
      List<ChangeRecord> records = batch.getRecords();      
      int total = records.size();
      int count = 0;
      
      while(count < total){
         int size = Math.min(group, total - count);

         if(size > 0) {
            DataWriter writer = new OutputStreamWriter(stream);
            DataRecordWriter encoder = new DataRecordWriter(writer);
            
            writer.writeInt(size); // how many records!
            
            for(int i = 0; i < size; i++) {
               ChangeRecord record = records.get(count++);
               ChangeRecordWriter builder = record.getWriter();
               
               builder.write(encoder, counter);
            }
            stream.flush();
         }         
      }
      stream.flush();
   }
}
