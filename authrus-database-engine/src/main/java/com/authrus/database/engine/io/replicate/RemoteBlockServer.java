package com.authrus.database.engine.io.replicate;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import com.authrus.database.common.thread.ThreadPoolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteBlockServer {

   private static final Logger LOG = LoggerFactory.getLogger(RemoteBlockServer.class);
   
   private final RemoteBlockHandler handler;
   private final SocketAcceptor acceptor;
   private final ThreadFactory factory;
   private final ServerSocket server;
   
   public RemoteBlockServer(RemoteBlockHandler handler, int port) throws IOException {
      this.factory = new ThreadPoolFactory(SocketAcceptor.class);      
      this.server = new ServerSocket(port);
      this.acceptor = new SocketAcceptor(server);
      this.handler = handler;
   }
   
   public void start() {
      if (!acceptor.isAlive()) {
         Thread thread = factory.newThread(acceptor);
         
         acceptor.start();
         thread.start();
      }
   }

   public void stop()  {
      if (acceptor.isAlive()) {
         acceptor.stop();
      }
   }   
   
   private class SocketAcceptor implements Runnable {
      
      private final ServerSocket server;
      private final AtomicBoolean alive;
      
      public SocketAcceptor(ServerSocket server) {
         this.alive = new AtomicBoolean();      
         this.server = server;
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

      @Override
      public void run() {
         try {
            while(alive.get()) {
               try {
                  Socket socket = server.accept();

                  if(socket.isConnected()) {
                     handler.connect(socket);  
                  } else {
                     socket.close();
                  }
               } catch(Exception e) {
                  LOG.info("Error accepting connection", e);
               }
            }
         } finally {
            alive.set(false);
         }
      }            
   }
}
