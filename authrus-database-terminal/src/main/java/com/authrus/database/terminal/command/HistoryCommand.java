package com.authrus.database.terminal.command;

import static java.util.Collections.EMPTY_LIST;

import java.util.List;

import com.authrus.database.terminal.session.SessionContext;

import com.google.common.collect.Lists;

public class HistoryCommand implements Command {

   @Override
   public CommandResult execute(SessionContext session, String expression, boolean execute) throws Exception{
      List<String> commands = session.getHistory();
      
      if(!commands.isEmpty()) {
         List<HistoryResult> results = Lists.newArrayList();
         
         for(String command : commands) {
            HistoryResult result = HistoryResult.builder()
               .command(command)
               .build();
            
            results.add(result);
         }
         return CommandResult.builder()
               .formatter(CollectionFormatter.class)
               .result(results)
               .build();
      }
      return CommandResult.builder()
            .formatter(CollectionFormatter.class)
            .result(EMPTY_LIST)
            .error("No history")
            .build(); 
   }

}
