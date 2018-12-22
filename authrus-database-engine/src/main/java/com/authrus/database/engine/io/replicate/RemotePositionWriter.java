package com.authrus.database.engine.io.replicate;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Set;

public class RemotePositionWriter {

   private final RemoteConnection connection;
   
   public RemotePositionWriter(RemoteConnection connection) {
      this.connection = connection;
   }
   
   public void writePosition(Position position) throws IOException {
      DataOutputStream output = connection.getOutputStream();
      Set<String> tables = position.getTables();
      int size = tables.size();
      
      output.writeInt(size);
      
      for(String table : tables) {
         long time = position.getTime(table);
         
         output.writeLong(time);
         output.writeUTF(table);
      }
      output.flush();
   }
}
