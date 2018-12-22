package com.authrus.database.engine.io;

import java.io.IOException;

public abstract class DataRecordProducer {
   
   private final ByteArrayCompressor compressor; 
   
   protected DataRecordProducer() {
      this.compressor = new ByteArrayCompressor(4); // insert 4 byte header
   }
   
   public void produce(byte[] array) throws IOException {
      produce(array, 0, array.length);
   }
   
   public void produce(byte[] array, int off, int length) throws IOException {
      byte[] result = compressor.write(array, off, length);
      int size = result.length - 4;
      
      result[0] = (byte)((size >>> 24) & 0xff);
      result[1] = (byte)((size >>> 16) & 0xff);
      result[2] = (byte)((size >>> 8) & 0xff);
      result[3] = (byte)((size >>> 0) & 0xff);

      write(result, 0, result.length);      
   }
   
   public abstract void write(byte[] array, int off, int length) throws IOException;
   public abstract void close() throws IOException;
}
