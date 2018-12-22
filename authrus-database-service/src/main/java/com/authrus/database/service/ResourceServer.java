package com.authrus.database.service;

import com.authrus.database.service.container.ContainerManager;
import com.authrus.database.service.container.ContainerManagerBuilder;
import com.authrus.database.service.container.DependencyManager;
import com.authrus.database.service.container.ServiceRouter;
import com.authrus.database.service.content.ContentHandlerMatcher;
import com.authrus.database.service.content.FileContentHandlerMatcher;

import java.io.File;

import lombok.SneakyThrows;

import org.glassfish.jersey.server.ResourceConfig;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

public class ResourceServer {
   
   private final ContainerManagerBuilder builder;
   private final ContentHandlerMatcher matcher;
   private final DependencyManager manager;
   private final int port;
   
   public ResourceServer(ServiceRouter router, String packages, String name, File directory, int port, boolean swagger) {
      this.matcher = new FileContentHandlerMatcher(directory);
      this.manager = new DependencyManager(packages, swagger);
      this.builder = new ContainerManagerBuilder(matcher, router, name);
      this.port = port;
   }

   @SneakyThrows
   public ContainerManager start(ApplicationContext context) {
      ResourceConfig config = manager.start((ConfigurableApplicationContext)context);
      return builder.create(config, null, port);
   }
}
