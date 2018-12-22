package com.authrus.database.engine.statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.authrus.database.Tracer;
import com.authrus.database.sql.Query;

public class QueryTimer implements Tracer<Query> {

   private static final Logger LOG = LoggerFactory.getLogger(QueryTimer.class);   
   
   private final boolean enabled;
   
   public QueryTimer(){
      this(false);
   }
   
   public QueryTimer(boolean enabled) {
      this.enabled = enabled;
   }

   public void trace(Query query, long duration) {
      String expression = query.getSource();
      
      if(enabled) {
         LOG.info("Query '" + expression + "' took " + duration + " milliseconds");
      }
   }
}
