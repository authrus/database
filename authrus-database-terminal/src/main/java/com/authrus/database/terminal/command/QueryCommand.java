package com.authrus.database.terminal.command;

import static java.util.Collections.EMPTY_LIST;

import com.authrus.database.terminal.session.SessionContext;

public class QueryCommand implements Command {
   
   private final GoCommand command;
   
   public QueryCommand() {
      this.command = new GoCommand();
   }

   @Override
   public CommandResult execute(SessionContext session, String expression, boolean execute) throws Exception {
      StringBuilder buffer = session.getBuffer();
      
      if(!execute) {
         buffer.append(expression);
         buffer.append(" ");      

         return CommandResult.builder()
               .formatter(CollectionFormatter.class)
               .result(EMPTY_LIST)
               .build();
      }
      buffer.setLength(0);
      buffer.append(expression);
      
      return command.execute(session, expression, execute);      
   }

}
