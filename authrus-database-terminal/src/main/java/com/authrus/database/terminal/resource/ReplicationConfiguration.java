package com.authrus.database.terminal.resource;

import java.util.concurrent.ThreadFactory;

import lombok.SneakyThrows;

import com.authrus.database.common.thread.ThreadPool;
import com.authrus.database.common.thread.ThreadPoolFactory;
import com.authrus.database.engine.Catalog;
import com.authrus.database.engine.io.read.CatalogAssembler;
import com.authrus.database.engine.io.read.ChangeAssembler;
import com.authrus.database.engine.io.read.ChangeScheduler;
import com.authrus.database.engine.io.read.ThreadPoolScheduler;
import com.authrus.database.engine.io.replicate.ChangeReplicator;
import com.authrus.database.engine.io.replicate.ChangeServer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(DatabaseConfiguration.class)
public class ReplicationConfiguration {
   
   private final String directory;
   private final String origin;
   private final String mirror;
   private final int port;
   
   public ReplicationConfiguration(
         @Value("${database.directory}") String directory,
         @Value("${database.origin:master}") String origin,
         @Value("${database.mirror}") String mirror,
         @Value("${database.port}") int port) 
   {
      this.directory = directory;
      this.mirror = mirror;
      this.origin = origin;
      this.port = port;
   }   
   
   @SneakyThrows
   @Bean(initMethod = "start")
   public ChangeReplicator changeReplicator(Catalog catalog) {
      ChangeAssembler assembler = new CatalogAssembler(catalog);
      ThreadFactory factory = new ThreadPoolFactory(ChangeScheduler.class);
      ThreadPool pool = new ThreadPool(factory, 1);
      ChangeScheduler executor = new ThreadPoolScheduler(assembler, pool);  
      
      return new ChangeReplicator(executor, origin, directory, mirror, port);           
   }

   @SneakyThrows
   @Bean(initMethod = "start", destroyMethod = "stop")
   public ChangeServer changeServer() {
      return new ChangeServer(directory, port);
   }
}
