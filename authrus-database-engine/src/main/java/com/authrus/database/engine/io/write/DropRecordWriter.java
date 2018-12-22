package com.authrus.database.engine.io.write;

import static com.authrus.database.engine.OperationType.DROP;

import java.io.IOException;

import com.authrus.database.engine.io.DataRecordCounter;
import com.authrus.database.engine.io.DataRecordWriter;

public class DropRecordWriter implements ChangeRecordWriter {
   
   private final String origin;
   
   public DropRecordWriter(String origin) {
      this.origin = origin;     
   }

   @Override
   public void write(DataRecordWriter writer, DataRecordCounter counter) throws IOException {
      writer.writeChar(DROP.code);
      writer.writeString(origin);      
   }
}
