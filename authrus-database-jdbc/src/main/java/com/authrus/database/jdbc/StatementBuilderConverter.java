package com.authrus.database.jdbc;

import static com.authrus.database.sql.Verb.BEGIN;
import static com.authrus.database.sql.Verb.COMMIT;
import static com.authrus.database.sql.Verb.CREATE_INDEX;
import static com.authrus.database.sql.Verb.CREATE_TABLE;
import static com.authrus.database.sql.Verb.DELETE;
import static com.authrus.database.sql.Verb.DROP_INDEX;
import static com.authrus.database.sql.Verb.DROP_TABLE;
import static com.authrus.database.sql.Verb.INSERT;
import static com.authrus.database.sql.Verb.INSERT_OR_IGNORE;
import static com.authrus.database.sql.Verb.ROLLBACK;
import static com.authrus.database.sql.Verb.SELECT;
import static com.authrus.database.sql.Verb.SELECT_DISTINCT;
import static com.authrus.database.sql.Verb.UPDATE;

import java.sql.Connection;

import com.authrus.database.ResultCache;
import com.authrus.database.Statement;
import com.authrus.database.sql.Query;
import com.authrus.database.sql.QueryConverter;
import com.authrus.database.sql.Verb;
import com.authrus.database.sql.compile.QueryCompiler;

public class StatementBuilderConverter implements QueryConverter<Statement> {
   
   private final StatementTracer tracer;
   private final QueryCompiler compiler;
   private final Connection connection;
   private final ResultCache cache;
   
   public StatementBuilderConverter(Connection connection, StatementTracer tracer, ResultCache cache, QueryCompiler compiler) {
      this.connection = connection;
      this.compiler = compiler;
      this.tracer = tracer;
      this.cache = cache;
   }

   @Override
   public Statement convert(Query query) {
      Verb verb = query.getVerb();      
      
      if(verb == SELECT) {
         return new SelectStatementBuilder(connection, tracer, cache, compiler, query);               
      }
      if(verb == SELECT_DISTINCT) {
         return new SelectStatementBuilder(connection, tracer, cache, compiler, query);               
      }
      if(verb == INSERT) {
         return new InsertStatementBuilder(connection, tracer, cache, compiler, query);               
      }
      if(verb == INSERT_OR_IGNORE) {
         return new InsertStatementBuilder(connection, tracer, cache, compiler, query);               
      }
      if(verb == UPDATE) {
         return new UpdateStatementBuilder(connection, tracer, cache, compiler, query);               
      }
      if(verb == DELETE) {
         return new DeleteStatementBuilder(connection, tracer, cache, compiler, query);               
      }
      if(verb == CREATE_TABLE) {
         return new StatementBuilder(connection, tracer, cache, compiler, query);               
      }
      if(verb == DROP_TABLE) {
         return new StatementBuilder(connection, tracer, cache, compiler, query);               
      }
      if(verb == CREATE_INDEX) {
         return new StatementBuilder(connection, tracer, cache, compiler, query);               
      }
      if(verb == DROP_INDEX) {
         return new StatementBuilder(connection, tracer, cache, compiler, query);               
      }   
      if(verb == BEGIN) {
         return new StatementBuilder(connection, tracer, cache, compiler, query);               
      }
      if(verb == COMMIT) {
         return new StatementBuilder(connection, tracer, cache, compiler, query);               
      }
      if(verb == ROLLBACK) {
         return new StatementBuilder(connection, tracer, cache, compiler, query);               
      }       
      return null;
   }

}
