package com.authrus.database.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatementTracer {
   
   private static final Logger LOG = LoggerFactory.getLogger(StatementTracer.class);   
   
   private final boolean enabled;
   
   public StatementTracer(){
      this(false);
   }
   
   public StatementTracer(boolean enabled) {
      this.enabled = enabled;
   }

   public void traceStatement(String statement, long duration) {
      if(enabled) {
         LOG.info("SQL [" + statement + "] took " + duration + " milliseconds");
      }
   }
}
