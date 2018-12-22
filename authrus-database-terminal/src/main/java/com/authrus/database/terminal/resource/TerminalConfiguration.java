package com.authrus.database.terminal.resource;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.authrus.database.Database;
import com.authrus.database.engine.Catalog;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(DatabaseConfiguration.class)
public class TerminalConfiguration {
   
   private final Executor executor;
   
   public TerminalConfiguration(@Value("${database.threads:20}") int threads) {    
      this.executor = threads <= 1 ? (task) -> task.run() : Executors.newFixedThreadPool(threads);
   }
   
   @Bean
   public TerminalService terminal(Database database, Catalog catalog) {                    
      return new TerminalService(database, catalog, executor);
   }
}
