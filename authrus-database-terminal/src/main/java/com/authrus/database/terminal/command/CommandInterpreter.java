package com.authrus.database.terminal.command;

import java.util.List;

import com.authrus.database.Database;
import com.authrus.database.engine.Catalog;
import com.authrus.database.terminal.session.SessionBuilder;
import com.authrus.database.terminal.session.SessionContext;
import com.authrus.database.terminal.session.SessionTime;
import com.google.common.collect.Lists;

public class CommandInterpreter {
   
   private final SessionBuilder builder;
   
   public CommandInterpreter(Database database, Catalog catalog){
      this.builder = new SessionBuilder(database, catalog);
   }
   
   public List<CommandResult> execute(SessionTime time, List<String> values, String user, boolean execute) throws Exception {
      List<CommandResult> results = Lists.newArrayList();
      SessionContext session = builder.create(time, user);
      StringBuilder buffer = session.getBuffer();
      
      for(String value : values) {
         CommandExpression expression = CommandExpression.resolveExpression(value);
         
         try {
            Command command = expression.getCommand();
            CommandResult result = command.execute(session, value, execute);
            
            results.add(result);
         } finally {
            if(expression.isReset()) {
               buffer.setLength(0);
            }
         }
      }
      return results;
   }      
   
}