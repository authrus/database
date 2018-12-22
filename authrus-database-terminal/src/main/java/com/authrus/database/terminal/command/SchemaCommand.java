package com.authrus.database.terminal.command;

import static java.util.Collections.EMPTY_LIST;

import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.authrus.database.Column;
import com.authrus.database.Schema;
import com.authrus.database.engine.Catalog;
import com.authrus.database.engine.Table;
import com.authrus.database.function.DefaultFunction;
import com.authrus.database.function.DefaultValue;
import com.authrus.database.terminal.session.SessionContext;

import com.google.common.collect.Lists;

public class SchemaCommand implements Command {
   
   private static final String COMMAND_PATTERN = "\\s*show\\s+table\\s+(.*)\\s*";

   @Override
   public CommandResult execute(SessionContext session, String text, boolean execute) throws Exception{
      Catalog catalog = session.getCatalog();
      
      if(catalog != null) {
         Pattern pattern = Pattern.compile(COMMAND_PATTERN, Pattern.CASE_INSENSITIVE);
         Matcher matcher = pattern.matcher(text);
         
         if(!matcher.matches()) {
            throw new IllegalArgumentException("Expression '" + text + "' is invalid");
         }
         String name = matcher.group(1);
         Table table = catalog.findTable(name);
         
         if(table != null) {
            List<SchemaResult> results = Lists.newArrayList();
            Schema schema = table.getSchema();        
            int width = schema.getCount();
            
            for(int i = 0; i < width;i++){
               Column column = schema.getColumn(i);
               DefaultValue value = column.getDefaultValue();
               DefaultFunction function = value.getFunction();
               SchemaResult result = SchemaResult.builder()
                     .name(column.getTitle())
                     .type(column.getDataType())
                     .function(function)
                     .build();
               
               results.add(result);                     
            }
            return CommandResult.builder()
                  .formatter(CollectionFormatter.class)
                  .result(results)
                  .build();
         }
      }
      return CommandResult.builder()
            .formatter(CollectionFormatter.class)
            .result(EMPTY_LIST)
            .error("Table not found")
            .build();
   }

}
