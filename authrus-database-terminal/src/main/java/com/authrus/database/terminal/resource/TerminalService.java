package com.authrus.database.terminal.resource;

import static com.authrus.database.terminal.resource.TerminalType.TEXT;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import com.authrus.database.Database;
import com.authrus.database.engine.Catalog;
import com.authrus.database.terminal.command.CommandFormatter;
import com.authrus.database.terminal.command.CommandInterpreter;
import com.authrus.database.terminal.command.CommandResult;
import com.authrus.database.terminal.session.SessionTime;
import com.authrus.database.terminal.session.SessionTimeParser;

public class TerminalService {

   private final CommandInterpreter interpreter;
   private final SessionTimeParser parser;   
   private final Executor executor;
   
   public TerminalService(Database database, Catalog catalog, Executor executor){
      this.interpreter = new CommandInterpreter(database, catalog);
      this.parser = new SessionTimeParser();
      this.executor = executor;
   }
   
   public CompletableFuture<TerminalResult> submit(TerminalRequest request) {
      CompletableFuture<TerminalResult> future = new CompletableFuture<>();      
      
      executor.execute(() -> {
         try {
            boolean execute = request.isExecute();
            String date = request.getDate();
            String text = request.getCommand();
            String user = request.getSession();
            TerminalType type = request.getType();
            SessionTime time = parser.parseTime(date);
            CommandResult result = interpreter.execute(time, user, text, execute);
            Object value = result.getResult();
            String error = result.getError();
            
            if(type != TEXT) {                       
               TerminalResult response = TerminalResult.builder()
                     .content(value)
                     .error(error)
                     .build();
               
               future.complete(response);
            } else {
               Class<? extends CommandFormatter> factory = result.getFormatter();
               CommandFormatter formatter = factory.newInstance();
               String output = formatter.format(text, value);
               TerminalResult response = TerminalResult.builder()
                     .content(output)
                     .error(error)                     
                     .build();
               
               future.complete(response);  
            }
         }catch(Exception cause){
            future.completeExceptionally(cause);
         }
      });
      return future;
   }
}
