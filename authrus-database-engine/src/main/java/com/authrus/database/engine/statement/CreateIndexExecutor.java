package com.authrus.database.engine.statement;

import java.util.Collections;
import java.util.List;

import com.authrus.database.Column;
import com.authrus.database.Record;
import com.authrus.database.RecordIterator;
import com.authrus.database.ResultIterator;
import com.authrus.database.Schema;
import com.authrus.database.engine.Catalog;
import com.authrus.database.engine.Table;
import com.authrus.database.engine.TableModel;
import com.authrus.database.sql.Query;

public class CreateIndexExecutor extends StatementExecutor {
   
   private final Catalog catalog;
   private final Query query;
   
   public CreateIndexExecutor(Catalog catalog, Query query) {
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
      String index = query.getName();
      String expression = query.getSource();
      Table table = catalog.findTable(name);
      
      if(table == null) {
         throw new IllegalStateException("Unable to create index '" + index + "' as table '" + table + "' does not exist");
      }
      List<String> columns = query.getColumns();
      Schema schema = table.getSchema();
      TableModel tableModel = table.getModel();
      int columnCount = columns.size();
      
      for(int i = 0; i < columnCount; i++) {
         String columnName = columns.get(i);
         Column column = schema.getColumn(columnName);
         
         if(table == null) {
            throw new IllegalStateException("Unable to create index '" + index + "' as column '" + columnName + "' does not exist");
         }
         tableModel.index(column);
      }
      return new RecordIterator(Collections.EMPTY_LIST, expression);      
   }
}
