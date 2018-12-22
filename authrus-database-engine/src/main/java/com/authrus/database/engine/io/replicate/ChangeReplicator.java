package com.authrus.database.engine.io.replicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.authrus.database.engine.io.read.ChangeScheduler;

public class ChangeReplicator {
   
   private static final Logger LOG = LoggerFactory.getLogger(ChangeReplicator.class);

   private final RemoteChangeReplicator replicator;
   private final ChangeLoader loader;
   private final Position position;
   
   public ChangeReplicator(ChangeScheduler executor, String origin, String directory,  String host, int port) {
      this(executor, origin, directory, host, port, 5000);
   }
   
   public ChangeReplicator(ChangeScheduler executor, String origin, String directory, String host, int port, long retry) {
      this.position = new Position();
      this.replicator = new RemoteChangeReplicator(executor, position, origin, host, port);
      this.loader = new ChangeLoader(executor, position, origin, directory);     
   }
   
   public void start() {
      try {
         loader.process();
         replicator.start();
      } catch(Exception e) {
         LOG.info("Error replicating catalog", e);               
      }      
   }
}
