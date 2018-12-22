package com.authrus.database.engine.io;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class OutputStreamRecordProducer extends DataRecordProducer {

   private final DataOutputStream output;

   public OutputStreamRecordProducer(OutputStream output) {
      this.output = new DataOutputStream(output);
   }

   @Override
   public void write(byte[] array, int off, int length) throws IOException {
      output.write(array, off, length);
      output.flush();
   }

   @Override
   public void close() throws IOException {
      output.close();
   }
}