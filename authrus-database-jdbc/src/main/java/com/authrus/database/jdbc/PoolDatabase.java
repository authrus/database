package com.authrus.database.jdbc;

import java.sql.Connection;

import javax.sql.DataSource;

import com.authrus.database.Database;
import com.authrus.database.DatabaseConnection;
import com.authrus.database.ResultCache;
import com.authrus.database.sql.QueryConverter;
import com.authrus.database.sql.build.QueryProcessor;
import com.authrus.database.sql.compile.QueryCompiler;

public class PoolDatabase implements Database {
   
   private final StatementTracer tracer;
   private final QueryCompiler compiler;
   private final DataSource source;
   private final ResultCache cache;
   
   public PoolDatabase(DataSource source, QueryCompiler compiler) {
      this(source, compiler, false);
   }
   
   public PoolDatabase(DataSource source, QueryCompiler compiler, boolean debug) {
      this.tracer = new StatementTracer(debug);
      this.cache = new ResultCache();
      this.compiler = compiler;
      this.source = source;    
   } 
   
   @Override
   public synchronized DatabaseConnection getConnection() throws Exception {
      Connection connection = source.getConnection();
      
      if(connection == null) {
         throw new IllegalStateException("Unable to acquire connection");
      }
      QueryConverter converter = new StatementBuilderConverter(connection, tracer, cache, compiler);
      QueryProcessor processor = new QueryProcessor(converter);
      
      return new PoolConnection(connection, processor);    
   }
}
