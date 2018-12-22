package com.authrus.database.terminal.resource;

import static com.authrus.database.terminal.resource.TerminalType.TEXT;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;

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
      TerminalType type = request.getType();
      
      if(type != TEXT) {
         return submit(request, (results) -> {
            return results.stream()
                  .map(CommandResult::getResult)
                  .filter(Objects::nonNull)                  
                  .collect(Collectors.toList());
         });
      }
      return submit(request, (results) -> {
         return results.stream()
            .map(result -> {
               try {
                  Class<? extends CommandFormatter> factory = result.getFormatter();
                  CommandFormatter formatter = factory.newInstance();
                  String expression = result.getExpression();
                  Object value = result.getResult();                           
                  
                  return formatter.format(expression, value);
               } catch(Exception e) {
                  return null;
               }
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
      });
   }
   
   private CompletableFuture<TerminalResult> submit(TerminalRequest request, Function<List<CommandResult>, List<Object>> function) {
      CompletableFuture<TerminalResult> future = new CompletableFuture<>();      
      
      executor.execute(() -> {
         try {
            boolean execute = request.isExecute();
            List<String> text = request.getCommands();
            String date = request.getDate();
            String user = request.getSession();
            SessionTime time = parser.parseTime(date); 
            List<CommandResult> results = interpreter.execute(time, text, user, execute);
            List<Object> values = function.apply(results);            
            TerminalResult response = TerminalResult.builder()
                  .content(values)
                  .build();
            
            future.complete(response);
         }catch(Exception cause){
            future.completeExceptionally(cause);
         }
      });
      return future;
   }
}
