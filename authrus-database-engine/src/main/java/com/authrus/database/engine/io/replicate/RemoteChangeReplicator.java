package com.authrus.database.engine.io.replicate;

import java.util.Iterator;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import com.authrus.database.common.thread.ThreadPoolFactory;
import com.authrus.database.engine.TransactionFilter;
import com.authrus.database.engine.io.DataRecord;
import com.authrus.database.engine.io.read.ChangeProcessor;
import com.authrus.database.engine.io.read.ChangeScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteChangeReplicator {  
   
   private static final Logger LOG = LoggerFactory.getLogger(RemoteChangeReplicator.class);
   
   private final ChangeProcessor processor;
   private final ChangeConsumer consumer;
   private final TransactionFilter filter;
   private final ThreadFactory factory;
   private final Position position;

   public RemoteChangeReplicator(ChangeScheduler executor, Position position, String origin, String host, int port) {
      this(executor, position, origin, host, port, 5000);
   }
   
   public RemoteChangeReplicator(ChangeScheduler executor, Position position, String origin, String host, int port, long retry) {
      this.factory = new ThreadPoolFactory(ChangeConsumer.class);
      this.consumer = new ChangeConsumer(host, port, retry);
      this.filter = new ReplicationFilter(position, origin);
      this.processor = new ChangeProcessor(executor, filter);
      this.position = position;

   }   
   
   public void start() {
      if (!consumer.isAlive()) {
         Thread thread = factory.newThread(consumer);
         
         consumer.start();
         thread.start();
      }
   }

   public void stop()  {
      if (consumer.isAlive()) {
         consumer.stop();
      }
   }

   private class ChangeConsumer implements Runnable {
      
      private final RemoteConnector connector;
      private final AtomicBoolean alive;
      private final String host;
      private final int port;
      private final long retry;

      public ChangeConsumer(String host, int port, long retry) {
         this.connector = new RemoteConnector(host, port);
         this.alive = new AtomicBoolean();
         this.retry = retry;
         this.host = host;
         this.port = port;
      }

      public boolean isAlive() {
         return alive.get();
      }
      
      public void start() {
         alive.set(true);
      }

      public void stop() {
         alive.set(false);
      }
      
      public void run() {
         try {
            while(alive.get()) {
               pause();
               consume();
            }
         } finally {
            alive.set(false);
         }
      }
      
      private void pause() {
         try {
            Thread.sleep(retry + 1); // never use 0
         } catch(Exception e) {
            LOG.info("Could not pause connector", e);
         }
      }
      
      private void consume() {
         try {
            Iterator<DataRecord> reader = connector.connect(position);
            
            try {
               while(alive.get()) {
                  int count = processor.process(reader); // this should never return!!
               
                  if(count > 0) {
                     LOG.info("Received " + count + " changes");
                  }
               }
            } catch(Exception e) {
               LOG.info("Error synchronizing changes", e);
            }
         } catch(Exception e) {
            LOG.info("Could not connect to " + host + ":" + port);
         }
      }
   }
}
