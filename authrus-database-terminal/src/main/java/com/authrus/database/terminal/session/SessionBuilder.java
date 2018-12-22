package com.authrus.database.terminal.session;
import com.authrus.database.Database;
import com.authrus.database.common.collection.Cache;
import com.authrus.database.common.collection.LeastRecentlyUsedCache;
import com.authrus.database.engine.Catalog;

public class SessionBuilder {
   
   private final Cache<String, SessionContext> sessions;
   private final MemoryAnalyzer analyzer;
   private final Database database;
   private final Catalog catalog;
   
   public SessionBuilder(Database database, Catalog catalog){
      this.sessions = new LeastRecentlyUsedCache<String, SessionContext>();
      this.analyzer = new MemoryAnalyzer(); // creates thread locals
      this.database = database;
      this.catalog = catalog;
   }
   
   public SessionContext create(SessionTime time, String user) {
      SessionContext session = sessions.fetch(user);
      
      if(session == null) {
         session = new SessionContext(time, analyzer, database, catalog);
         sessions.cache(user, session);
      }
      return session;
   }      
   
}