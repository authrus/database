package com.authrus.database.engine.io;

import java.io.IOException;

public class FileRecordProducer extends DataRecordProducer {

   private final FilePointer pointer;
   
   public FileRecordProducer(FilePointer pointer) {
      this.pointer = pointer;
   }
   
   @Override
   public void write(byte[] array, int off, int length) throws IOException {
      FileHandle handle = pointer.handle();
      FileAppender appender = handle.open();

      if(appender != null) {
         appender.append(array, off, length);
      }      
   }
   
   @Override
   public void close() throws IOException {
      FileHandle handle = pointer.handle();
      FileAppender appender = handle.open();
      
      if(appender != null) {
         appender.close();
      }
   }
}
