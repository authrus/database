package com.authrus.database.engine.statement;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.authrus.database.Record;
import com.authrus.database.RecordIterator;
import com.authrus.database.ResultIterator;
import com.authrus.database.engine.Catalog;
import com.authrus.database.engine.Table;
import com.authrus.database.engine.TableModel;
import com.authrus.database.engine.filter.Filter;
import com.authrus.database.engine.filter.FilterBuilder;
import com.authrus.database.sql.Query;

public class DeleteExecutor extends StatementExecutor {

   private final FilterBuilder builder;
   private final Catalog catalog;
   private final Query query;
   
   public DeleteExecutor(Catalog catalog, Query query) {
      this.builder = new FilterBuilder(catalog, query);
      this.catalog = catalog;
      this.query = query;   
   }
   
   @Override
   public ResultIterator<Record> execute() throws Exception {
      boolean disposed = closed.get();
      
      if(disposed) {
         throw new IllegalStateException("This statement has been closed");
      }
      String name = query.getTable();    
      String expression = query.getSource();
      Filter deleteFilter = createFilter();
      Table table = catalog.findTable(name);
      TableModel tableModel = table.getModel();
      
      if(tableModel != null) {
         tableModel.remove(deleteFilter);
      }
      return new RecordIterator(Collections.EMPTY_LIST, expression);      
   }      
   
   private Filter createFilter() throws Exception {
      Map<String, String> values = new HashMap<String, String>();
      
      if(!attributes.isEmpty()) {
         Set<String> names = attributes.keySet();
         
         for(String name : names) {
            Object value = attributes.get(name);
            String text = null;
            
            if(value != null) {
               text = String.valueOf(value);
            }
            values.put(name, text);
         }
      }
      return builder.createFilter(values);      
   }    
}
