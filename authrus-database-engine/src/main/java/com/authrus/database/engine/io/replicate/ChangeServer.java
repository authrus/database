package com.authrus.database.engine.io.replicate;

import java.io.IOException;

public class ChangeServer {

   private final RemoteBlockHandler handler;
   private final RemoteBlockServer server;
   
   public ChangeServer(String directory, int port)  throws IOException {
      this(directory, port, 10000);
   }
   
   public ChangeServer(String directory, int port, long wait) throws IOException {
      this.handler = new RemoteBlockHandler(directory, wait);
      this.server = new RemoteBlockServer(handler, port);      
   }
   
   public void start() {
      server.start();
   }

   public void stop()  {
      server.stop();
   }  
}
