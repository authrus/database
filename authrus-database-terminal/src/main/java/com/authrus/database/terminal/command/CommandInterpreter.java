package com.authrus.database.terminal.command;

import com.authrus.database.Database;
import com.authrus.database.engine.Catalog;
import com.authrus.database.terminal.session.SessionBuilder;
import com.authrus.database.terminal.session.SessionContext;
import com.authrus.database.terminal.session.SessionTime;

public class CommandInterpreter {
   
   private final SessionBuilder builder;
   
   public CommandInterpreter(Database database, Catalog catalog){
      this.builder = new SessionBuilder(database, catalog);
   }
   
   public CommandResult execute(SessionTime time, String user, String text, boolean execute) throws Exception {
      SessionContext session = builder.create(time, user);
      StringBuilder buffer = session.getBuffer();
      CommandExpression expression = CommandExpression.resolveExpression(text);
      Command command = expression.getCommand();
      
      try {
         return command.execute(session, text, execute);
      } finally {
         if(expression.isReset()) {
            buffer.setLength(0);
         }
      }
   }      
   
}