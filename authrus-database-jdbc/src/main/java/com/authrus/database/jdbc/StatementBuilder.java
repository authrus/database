package com.authrus.database.jdbc;

import static java.util.Collections.EMPTY_LIST;

import java.sql.Connection;

import com.authrus.database.Record;
import com.authrus.database.RecordIterator;
import com.authrus.database.ResultCache;
import com.authrus.database.ResultIterator;
import com.authrus.database.StatementTemplate;
import com.authrus.database.sql.Query;
import com.authrus.database.sql.compile.QueryCompiler;

public class StatementBuilder extends StatementTemplate {
   
   protected static final Object[] EMPTY = {};
   
   protected final StatementTracer tracer;
   protected final QueryCompiler compiler;
   protected final Connection connection;
   protected final ResultCache cache;
   protected final Query query;
   
   public StatementBuilder(Connection connection, StatementTracer tracer, ResultCache cache, QueryCompiler compiler, Query query) {
      this.connection = connection;
      this.compiler = compiler;
      this.tracer = tracer;
      this.cache = cache;
      this.query = query;   
   }
   
   @Override
   public synchronized ResultIterator<Record> execute() throws Exception {
      long start = System.currentTimeMillis();
      boolean disposed = closed.get();
      
      if(disposed) {
         throw new IllegalStateException("This statement has been closed");
      }    
      String table = query.getTable();
      String expression = compiler.compile(query, EMPTY);
      java.sql.Statement statement = connection.createStatement();      
      
      try {
         try {
            statement.execute(expression);
            cache.clear(table);
         } finally {
            statement.close();
         }   
         return new RecordIterator(EMPTY_LIST, expression);
      } finally {
         long finish = System.currentTimeMillis();
         long duration = finish - start;
         
         tracer.traceStatement(expression, duration);
      }
   } 
}
