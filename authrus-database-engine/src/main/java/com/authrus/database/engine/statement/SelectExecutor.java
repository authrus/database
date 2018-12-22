package com.authrus.database.engine.statement;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.authrus.database.Column;
import com.authrus.database.CountIterator;
import com.authrus.database.Record;
import com.authrus.database.ResultIterator;
import com.authrus.database.Schema;
import com.authrus.database.engine.Catalog;
import com.authrus.database.engine.Row;
import com.authrus.database.engine.RowResultIterator;
import com.authrus.database.engine.Table;
import com.authrus.database.engine.TableModel;
import com.authrus.database.engine.filter.Filter;
import com.authrus.database.engine.filter.FilterBuilder;
import com.authrus.database.sql.Query;

public class SelectExecutor extends StatementExecutor {

   private final FilterBuilder builder;
   private final Catalog catalog;
   private final Query query;
   
   public SelectExecutor(Catalog catalog, Query query) {
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
      
      if(name == null) {
         throw new IllegalStateException("Select statement '" + expression + "' must specify table");
      }
      Table table = catalog.findTable(name);      
      
      if(table == null) {
         throw new IllegalStateException("Select for '" + expression + "' references unknown table '" + name + "'");
      }
      Set<String> projection = createSchema();
      int width = projection.size();
      
      if(width == 1) {
         Iterator<String> iterator = projection.iterator();
         String column = iterator.next();
         
         if(column.equalsIgnoreCase("count(*)")) {
            return countRows(column);
         }
      }
      Filter filter = createFilter(); 
      Schema schema = table.getSchema();
      TableModel model = table.getModel();
      List<Row> matches = model.list(filter);  

      return new RowResultIterator(schema, matches, projection, expression);  
   }
   
   private ResultIterator<Record> countRows(String column) throws Exception {
      String name = query.getTable();  
      Table table = catalog.findTable(name);      
      TableModel model = table.getModel();
      Filter filter = createFilter(); 
      int count = model.count(filter);
      
      return new CountIterator(column, count);
   }
   
   private Set<String> createSchema() throws Exception {
      String name = query.getTable();
      String expression = query.getSource();
      Table table = catalog.findTable(name);
      Schema schema = table.getSchema();
      List<String> projection = query.getColumns();
      int width = projection.size();
      
      if(width == 0) {
         projection = schema.getColumns();
      }
      Set<String> columns = new LinkedHashSet<String>();      
      
      for(String entry : projection) {
         if(!entry.equalsIgnoreCase("count(*)")) {
            Column column = schema.getColumn(entry);
            
            if(column == null) {
               throw new IllegalStateException("Select for '" + expression + "' references unknown column '" + entry + "'");
            }
         }
         columns.add(entry);
      }
      return columns;
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
