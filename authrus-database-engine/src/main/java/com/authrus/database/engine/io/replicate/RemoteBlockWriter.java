package com.authrus.database.engine.io.replicate;

import java.io.DataOutputStream;
import java.io.IOException;

import com.authrus.database.engine.io.DataBlock;

public class RemoteBlockWriter {

   private final RemoteConnection connection;
   
   public RemoteBlockWriter(RemoteConnection connection) {
      this.connection = connection;
   }
   
   public void writeBlock(DataBlock block) throws IOException {
      DataOutputStream output = connection.getOutputStream();
      
      if(block != null) {
         String name = block.getName();
         byte[] data = block.getData();
         long time = block.getTime();
         int length = block.getLength();
         int offset = block.getOffset();
         
         output.writeBoolean(true);
         output.writeUTF(name);
         output.writeLong(time);
         output.writeInt(length);
         output.write(data, offset, length);
      } else {
         output.writeBoolean(false);
      }
      output.flush();
   }
}
