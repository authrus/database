package com.authrus.database.engine.io.replicate;

import java.io.DataOutputStream;
import java.util.concurrent.atomic.AtomicLong;

import com.authrus.database.engine.io.DataBlock;
import com.authrus.database.engine.io.DataBlockConsumer;

public class RemoteBlockConsumer implements DataBlockConsumer {
   
   private final RemotePositionWriter writer;
   private final RemoteConnection connection;
   private final RemoteBlockReader reader;   
   private final AtomicLong counter;
   private final Position position;

   public RemoteBlockConsumer(RemoteConnection connection, Position position) {
      this.writer = new RemotePositionWriter(connection);
      this.reader = new RemoteBlockReader(connection);
      this.counter = new AtomicLong();
      this.connection = connection;
      this.position = position;    
   }

   @Override
   public DataBlock read(long wait) {
      try {
         DataOutputStream output = connection.getOutputStream(); // needs to be closed on error       
         long total = counter.get();
         
         if(total == 0) {
            writer.writePosition(position);
         } else {
            output.writeLong(total);
            output.flush();
         }         
         while(true) {
            DataBlock block = reader.readBlock();
            
            if(block != null) {
               int length = block.getLength();
               
               if(length > 0) {
                  counter.getAndAdd(length);
               }
               return block;
            } else {
               output.writeLong(total); // ping when no block
               output.flush();
            }
         }
      } catch (Exception e) {
         throw new IllegalStateException("Error waiting for packet on " + connection, e);
      }  
   }   
}