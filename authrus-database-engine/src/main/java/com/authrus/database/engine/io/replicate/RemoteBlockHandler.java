package com.authrus.database.engine.io.replicate;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import com.authrus.database.common.thread.ThreadPool;
import com.authrus.database.common.thread.ThreadPoolFactory;
import com.authrus.database.engine.io.DataBlock;
import com.authrus.database.engine.io.FileBlockConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteBlockHandler {   

   private static final Logger LOG = LoggerFactory.getLogger(RemoteBlockHandler.class);

   private final ThreadFactory factory;
   private final ThreadPool pool;
   private final String directory;
   private final long wait;

   public RemoteBlockHandler(String directory) {
      this(directory, Integer.MAX_VALUE);
   }
   
   public RemoteBlockHandler(String directory, long wait) {
      this.factory = new ThreadPoolFactory(BlockForwarder.class);
      this.pool = new ThreadPool(factory, 5);
      this.directory = directory;
      this.wait = wait;
   }
   
   public void connect(Socket socket) throws IOException {
      Position position = new Position();
      PositionSeeker filter = new PositionSeeker(position);
      RemoteConnection connection = new RemoteSocket(socket);
      RemotePositionReader reader = new RemotePositionReader(connection);
      FileBlockConsumer consumer = new FileBlockConsumer(filter, directory);
      BlockForwarder forwarder = new BlockForwarder(connection, consumer);
      
      socket.setSoTimeout(60000);
      reader.readPosition(position);
      consumer.start();
      pool.execute(forwarder);     
   }
   
   private class BlockForwarder extends Thread {
      
      private final FileBlockConsumer consumer;
      private final RemoteConnection connection;
      private final RemoteBlockWriter writer;
      private final AtomicLong counter;
      
      public BlockForwarder(RemoteConnection connection, FileBlockConsumer consumer) {
         this.writer = new RemoteBlockWriter(connection);
         this.counter = new AtomicLong();
         this.connection = connection;
         this.consumer = consumer;
      }      
      
      @Override
      public void run() {
         try {
            while(true) {
               DataOutputStream output = connection.getOutputStream();
               DataInputStream input = connection.getInputStream();
               DataBlock block = consumer.read(wait);
               
               if(block != null) {
                  int length = block.getLength();
                  
                  writer.writeBlock(block);
                  counter.getAndAdd(length);
               } else {
                  output.writeBoolean(false); // send ping
                  output.flush();
               }
               long total = counter.get();               
               
               while(true) {
                  long progress = input.readLong();
               
                  if(progress > total) {
                     throw new IOException("Client progress is " + progress +" but total is " + total);
                  }
                  if(progress == total) {
                     break;
                  }
               }
            }        
         } catch(Exception e) {
            LOG.info("Error reading data", e);
         } finally {    
            consumer.stop();
         }
      }      
   }
}
