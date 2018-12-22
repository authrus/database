package com.authrus.database.engine;

import com.authrus.database.Database;
import com.authrus.database.DatabaseConnection;
import com.authrus.database.Statement;
import com.authrus.database.engine.statement.StatementExecutorConverter;
import com.authrus.database.engine.statement.StatementExecutorTimer;
import com.authrus.database.sql.QueryConverter;
import com.authrus.database.sql.build.QueryProcessor;

public class LocalDatabase implements Database {
   
   private final QueryConverter<Statement> converter;
   private final QueryConverter<Statement> timer;
   private final QueryProcessor processor;
   
   public LocalDatabase(Catalog catalog, String origin) {
      this(catalog, origin, false);
   }
   
   public LocalDatabase(Catalog catalog, String origin, boolean debug) {
      this.converter = new StatementExecutorConverter(catalog, origin);
      this.timer = new StatementExecutorTimer(converter, debug);
      this.processor = new QueryProcessor(timer);    
   }
   
   @Override
   public DatabaseConnection getConnection() throws Exception {
      return new LocalDatabaseConnection(processor);  
   }
}
