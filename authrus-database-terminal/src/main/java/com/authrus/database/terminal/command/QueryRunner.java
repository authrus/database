package com.authrus.database.terminal.command;

import java.util.List;

import com.authrus.database.Database;
import com.authrus.database.DatabaseConnection;
import com.authrus.database.terminal.session.SessionContext;

public class QueryRunner {

   private final QueryRequestBuilder builder;
   private final QueryExecutor executor;

   public QueryRunner() {
      this.executor = new QueryExecutor();
      this.builder = new QueryRequestBuilder();
   }

   public QueryResult run(SessionContext session, String source) throws Exception {
      Database database = session.getDatabase();
      DatabaseConnection connection = database.getConnection();
      
      try {
         List<String> commands = session.getHistory();
         QueryRequest request = builder.createRequest(source);
         QueryResult result = executor.execute(session, request);
         String expression = request.getExpression();
         
         commands.add(expression);
         return result;
      } finally {         
         connection.closeConnection();
      }
   }
}
