package com.authrus.database.engine.statement;

import com.authrus.database.Statement;
import com.authrus.database.StatementTracer;
import com.authrus.database.Tracer;
import com.authrus.database.sql.Query;
import com.authrus.database.sql.QueryConverter;

public class StatementExecutorTimer implements QueryConverter<Statement> {
   
   private final QueryConverter<Statement> converter;
   private final Tracer<Query> tracer;
   
   public StatementExecutorTimer(QueryConverter<Statement> converter) {
      this(converter, false);
   }
   
   public StatementExecutorTimer(QueryConverter<Statement> converter, boolean enable) {
      this.tracer = new QueryTimer(enable);
      this.converter = converter;     
   }

   @Override
   public Statement convert(Query query) {
      Statement statement = converter.convert(query);
      
      if(statement != null) {
         return new StatementTracer<Query>(statement, tracer, query);
      }
      return null;
   }

}
