package com.authrus.database.engine.io.replicate;

import java.io.DataInputStream;
import java.io.IOException;

import com.authrus.database.engine.io.DataBlock;

public class RemoteBlockReader {

   private final RemoteConnection connection;
   
   public RemoteBlockReader(RemoteConnection connection) {
      this.connection = connection;
   }
   
   public DataBlock readBlock() throws IOException {
      DataInputStream input = connection.getInputStream();
      
      if(input.readBoolean()) {
         String name = input.readUTF();
         long time = input.readLong();
         int length = input.readInt();
         int offset = 0;
         
         if(length > 0) {
            byte[] buffer = new byte[length];         
            
            while(offset < length) {                                                  
               int count = input.read(buffer, offset, length - offset);
         
               if(count == -1) {
                  break;
               }
               offset += count;            
            } 
            return new RemoteBlock(name, time, buffer, 0, length);
         }
      }
      return null;
   }
}
