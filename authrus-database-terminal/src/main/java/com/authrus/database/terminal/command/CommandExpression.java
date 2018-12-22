package com.authrus.database.terminal.command;

public enum CommandExpression {
   HISTORY(HistoryCommand.class, "history", "show history"),
   CATALOG(CatalogCommand.class, "show tables"),
   SCHEMA(SchemaCommand.class, "show table"),
   GO(GoCommand.class, "go"),
   REPEAT(RepeatCommand.class, "!"),   
   QUERY(QueryCommand.class, "");  
   
   private final Class<? extends Command> factory;
   private final String[] matches;
   
   private CommandExpression(Class<? extends Command> factory, String... matches) {
      this.factory = factory;
      this.matches = matches;
   }
   
   public Command getCommand() {
      try {
         return factory.newInstance();
      } catch(Exception e) {
         throw new IllegalStateException("Could not create", e);
      }
   }
   
   public boolean isReset() {
      return this != QUERY;
   }  
   
   public static CommandExpression resolveExpression(String text) {
      if(text != null) {
         CommandExpression[] expressions = CommandExpression.values();
         
         for(CommandExpression expression : expressions) {
            for(String prefix : expression.matches) {
               if(text.startsWith(prefix)) {
                  return expression;
               }
            }
         }         
      }
      return QUERY;      
   }
}
