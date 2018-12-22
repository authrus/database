package com.authrus.database.engine.io.write;

import static com.authrus.database.engine.OperationType.INDEX;

import java.io.IOException;

import com.authrus.database.engine.io.DataRecordCounter;
import com.authrus.database.engine.io.DataRecordWriter;

public class IndexRecordWriter implements ChangeRecordWriter {
   
   private final String origin;
   private final String column;
   
   public IndexRecordWriter(String origin, String column) {
      this.origin = origin;
      this.column = column;
   }

   @Override
   public void write(DataRecordWriter writer, DataRecordCounter counter) throws IOException {
      if(column == null) {
         throw new IllegalStateException("Column index does not specify a column");
      }
      writer.writeChar(INDEX.code);
      writer.writeString(origin);      
      writer.writeString(column);
   }
}
