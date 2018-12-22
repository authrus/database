package com.authrus.database.terminal.command;

import static java.util.Collections.EMPTY_LIST;

import java.util.List;
import java.util.Set;

import com.authrus.database.engine.Catalog;
import com.authrus.database.engine.Table;
import com.authrus.database.engine.TableModel;
import com.authrus.database.terminal.session.SessionContext;

import com.google.common.collect.Lists;

public class CatalogCommand implements Command {

   @Override
   public CommandResult execute(SessionContext session, String expression, boolean execute) throws Exception {
      Catalog catalog = session.getCatalog();
      
      if(catalog != null) {
         Set<String> tables = catalog.listTables();
                      
         if(!tables.isEmpty()) {
            List<CatalogResult> results = Lists.newArrayList();
            
            for(String entry : tables){
               Table tab = catalog.findTable(entry);
               TableModel model = tab.getModel();
               int rows = model.size();
               CatalogResult result = CatalogResult.builder()
                     .name(entry)
                     .rows(rows)
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
            .error("No tables")
            .build();    
   }

}
