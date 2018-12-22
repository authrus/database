package com.authrus.database.terminal.resource;

import lombok.SneakyThrows;

import com.authrus.database.Database;
import com.authrus.database.engine.Catalog;
import com.authrus.database.engine.ChangeListener;
import com.authrus.database.engine.LocalDatabase;
import com.authrus.database.engine.io.write.ChangeLogPersister;
import com.authrus.database.engine.io.write.FileLog;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DatabaseConfiguration {
   
   private final String directory;
   private final String origin;
   
   public DatabaseConfiguration(
         @Value("${database.directory}") String directory,
         @Value("${database.origin:master}") String origin) 
   {
      this.directory = directory;
      this.origin = origin;
   }
   

   @SneakyThrows
   @Bean(initMethod = "start", destroyMethod = "stop")
   public FileLog fileLog() {
      return new FileLog(directory, origin, 1024 * 1024 * 10, 10000);  
   }

   @Bean
   @SneakyThrows
   public Catalog catalog(FileLog log) {
      ChangeListener listener = new ChangeLogPersister(log);      
      return new Catalog(listener, origin);      
   }
   
   @Bean
   public Database database(Catalog catalog) {                    
      return new LocalDatabase(catalog, origin);
   }
   
   @Bean
   public DatabaseService databaseService(Database database, Catalog catalog) {                    
      return new DatabaseService(database, catalog);
   }
}
