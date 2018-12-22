package com.authrus.database.engine.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class DataRecordOutputStream extends ByteArrayOutputStream {

   private final DataRecordProducer producer;

   public DataRecordOutputStream(DataRecordProducer producer) {
      this.producer = producer;
   }

   @Override
   public void flush() throws IOException {
      if (count > 0) {
         producer.produce(buf, 0, count);
      }
      reset();
   }


}
