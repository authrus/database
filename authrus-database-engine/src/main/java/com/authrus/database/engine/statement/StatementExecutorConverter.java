package com.authrus.database.engine.statement;

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

import com.authrus.database.Statement;
import com.authrus.database.engine.Catalog;
import com.authrus.database.sql.Query;
import com.authrus.database.sql.QueryConverter;
import com.authrus.database.sql.Verb;

public class StatementExecutorConverter implements QueryConverter<Statement> {
   
   private final Catalog catalog;
   private final String origin;
   
   public StatementExecutorConverter(Catalog catalog, String origin) {
      this.catalog = catalog;
      this.origin = origin;
   }

   @Override
   public Statement convert(Query query) {
      Verb verb = query.getVerb();      
      
      if(verb == SELECT) {
         return new SelectExecutor(catalog, query);               
      }
      if(verb == SELECT_DISTINCT) {
         return new SelectExecutor(catalog, query);               
      }
      if(verb == INSERT) {
         return new InsertExecutor(catalog, query, true);               
      }
      if(verb == INSERT_OR_IGNORE) {
         return new InsertExecutor(catalog, query, false);               
      }
      if(verb == UPDATE) {
         return new UpdateExecutor(catalog, query);               
      }
      if(verb == DELETE) {
         return new DeleteExecutor(catalog, query);               
      }
      if(verb == CREATE_TABLE) {
         return new CreateTableExecutor(catalog, query, origin);               
      }
      if(verb == DROP_TABLE) {
         return new DropTableExecutor(catalog, query, origin);               
      }      
      if(verb == CREATE_INDEX) {
         return new CreateIndexExecutor(catalog, query);               
      }
      if(verb == DROP_INDEX) {
         return new DropIndexExecutor(catalog, query);               
      }
      if(verb == BEGIN) {
         return new BeginExecutor(catalog, query, origin);               
      }      
      if(verb == COMMIT) {
         return new CommitExecutor(catalog, query, origin);               
      }
      if(verb == ROLLBACK) {
         return new RollbackExecutor(catalog, query, origin);               
      }      
      return null;
   }

}
