package com.authrus.database.engine.io.write;

import static com.authrus.database.engine.OperationType.ROLLBACK;

import java.io.IOException;

import com.authrus.database.engine.io.DataRecordCounter;
import com.authrus.database.engine.io.DataRecordWriter;

public class RollbackRecordWriter implements ChangeRecordWriter {
   
   private final String origin;  
   
   public RollbackRecordWriter(String origin) {
      this.origin = origin;     
   }

   @Override
   public void write(DataRecordWriter writer, DataRecordCounter counter) throws IOException {
      writer.writeChar(ROLLBACK.code);
      writer.writeString(origin);      
   }
}
