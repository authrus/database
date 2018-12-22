package com.authrus.database.engine.io.write;

import static com.authrus.database.engine.OperationType.DELETE;

import java.io.IOException;

import com.authrus.database.engine.io.DataRecordCounter;
import com.authrus.database.engine.io.DataRecordWriter;

public class DeleteRecordWriter implements ChangeRecordWriter {
   
   private final String origin;
   private final String key;
   
   public DeleteRecordWriter(String origin, String key) {
      this.origin = origin;
      this.key = key;
   }

   @Override
   public void write(DataRecordWriter writer, DataRecordCounter counter) throws IOException {
      if(key == null) {
         throw new IllegalStateException("Delete does not specify a row");
      }
      writer.writeChar(DELETE.code);
      writer.writeString(origin);
      writer.writeString(key);
   }
}
