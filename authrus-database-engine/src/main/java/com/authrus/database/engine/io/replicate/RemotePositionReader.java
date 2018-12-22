package com.authrus.database.engine.io.replicate;

import java.io.DataInputStream;
import java.io.IOException;

public class RemotePositionReader {

   private final RemoteConnection connection;
   
   public RemotePositionReader(RemoteConnection connection) {
      this.connection = connection;
   }
   
   public void readPosition(Position position) throws IOException {
      DataInputStream input = connection.getInputStream(); 
      int count = input.readInt();
      
      for(int i = 0; i < count; i++) {
         long time = input.readLong();
         String table = input.readUTF();
         
         position.setTime(table, time);
      }
   }
}
