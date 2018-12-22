package com.authrus.database.terminal.command;

import static java.util.Collections.EMPTY_LIST;

import com.authrus.database.terminal.session.SessionContext;

public class GoCommand implements Command {

   private final QueryRunner runner;

   public GoCommand() {
      this.runner = new QueryRunner();
   }

   @Override
   public CommandResult execute(SessionContext session, String text, boolean execute) throws Exception {
      StringBuilder buffer = session.getBuffer();
      String source = buffer.toString();
      
      buffer.setLength(0);    
     
      try {
         QueryResult result = runner.run(session, source);
         return CommandResult.builder()
               .formatter(QueryResultFormatter.class)
               .expression(source)
               .result(result)
               .build();
      } catch (Exception cause) {
         String error = cause.getMessage();
         return CommandResult.builder()
               .formatter(QueryResultFormatter.class)
               .result(EMPTY_LIST)
               .error(error)
               .build();
      }
   }

}
