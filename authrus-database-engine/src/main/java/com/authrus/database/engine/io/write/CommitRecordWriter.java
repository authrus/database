package com.authrus.database.engine.io.write;

import static com.authrus.database.engine.OperationType.COMMIT;

import java.io.IOException;

import com.authrus.database.engine.io.DataRecordCounter;
import com.authrus.database.engine.io.DataRecordWriter;

public class CommitRecordWriter implements ChangeRecordWriter {
   
   private final String origin;  
   
   public CommitRecordWriter(String origin) {
      this.origin = origin;     
   }

   @Override
   public void write(DataRecordWriter writer, DataRecordCounter counter) throws IOException {
      writer.writeChar(COMMIT.code);
      writer.writeString(origin);      
   }
}
