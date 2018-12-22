package com.authrus.database.sql.compile;

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

import java.util.Collections;
import java.util.Map;

import com.authrus.database.sql.Query;
import com.authrus.database.sql.Verb;

public class QueryCompiler {
   
   protected static final Object[] EMPTY = {};
   
   protected Map<String, String> translations;
   
   public QueryCompiler() {
      this(Collections.EMPTY_MAP);
   }
   
   public QueryCompiler(Map<String, String> translations) {
      this.translations = translations;
   }
   
   public String compile(Query query) throws Exception {
      return compile(query, EMPTY);
   }
   
   public String compile(Query query, Object[] values) throws Exception {
      QueryCompiler compiler = resolve(query);
      
      if(compiler != null) {
         return compiler.compile(query, values);
      }
      return query.getSource();
   }
   
   
   protected String translate(String token) throws Exception {      
      if(token != null) {
         String key = token.toLowerCase();
         
         if(translations.containsKey(key)) {
            return translations.get(key);
         }
      }
      return token; 
   }
   
   protected String quote(String text) throws Exception {
      int quote = text.indexOf('\'');
      
      if(quote != -1) {
         text = text.replace("'", "''"); // ANSI SQL escape!
      }
      return "'" + text + "'";
   }
   
   protected QueryCompiler resolve(Query query) {
      Verb verb = query.getVerb();
      
      if(verb == SELECT) {
         return new SelectCompiler(translations);
      }
      if(verb == SELECT_DISTINCT) {
         return new SelectCompiler(translations, true);
      }
      if(verb == UPDATE) {
         return new UpdateCompiler(translations);
      }
      if(verb == INSERT) {
         return new InsertCompiler(translations);
      }
      if(verb == INSERT_OR_IGNORE) {
         return new InsertCompiler(translations);
      }
      if(verb == DELETE) {
         return new DeleteCompiler(translations);
      }
      if(verb == CREATE_TABLE) {
         return new CreateTableCompiler(translations);
      }
      if(verb == CREATE_INDEX) {
         return new CreateIndexCompiler(translations);
      }
      if(verb == DROP_TABLE) {
         return new DropTableCompiler(translations);
      }
      if(verb == DROP_INDEX) {
         return new DropIndexCompiler(translations);
      }
      if(verb == BEGIN) {
         return new BeginCompiler(translations);
      }
      if(verb == COMMIT) {
         return new CommitCompiler(translations);
      }
      if(verb == ROLLBACK) {
         return new RollbackCompiler(translations);
      }      
      return null;
   }
}
