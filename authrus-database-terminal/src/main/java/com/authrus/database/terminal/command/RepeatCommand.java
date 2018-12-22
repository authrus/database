package com.authrus.database.terminal.command;

import static java.util.Collections.EMPTY_LIST;

import java.util.List;

import com.authrus.database.terminal.session.SessionContext;

public class RepeatCommand implements Command {
   
   private final GoCommand command;
   
   public RepeatCommand(){
      this.command = new GoCommand();
   }

   @Override
   public CommandResult execute(SessionContext session, String expression, boolean execute) throws Exception{
      List<String> commands = session.getHistory();
      
      if(!commands.isEmpty() && !expression.isEmpty()) {
         String suffix = expression.trim().substring(1);
         int match = commands.size(); // last one
         int index = 1;
         
         if(!suffix.isEmpty()){
            match = Integer.parseInt(suffix);
         }
         for(String text : commands){
            if(match == index){
               StringBuilder builder = session.getBuffer();
               
               builder.setLength(0);
               builder.append(text);
               
               return command.execute(session, text, execute);               
            }
            index++;
         }
      }
      return CommandResult.builder()
            .formatter(CollectionFormatter.class)
            .result(EMPTY_LIST)
            .error("No history")
            .build();
   }
}
