package com.authrus.database.engine.statement;

import java.util.Collections;

import com.authrus.database.Record;
import com.authrus.database.RecordIterator;
import com.authrus.database.ResultIterator;
import com.authrus.database.engine.Catalog;
import com.authrus.database.engine.Table;
import com.authrus.database.sql.Query;

public class DropIndexExecutor extends StatementExecutor {
   
   private final Catalog catalog;
   private final Query query;
   
   public DropIndexExecutor(Catalog catalog, Query query) {
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
         throw new IllegalStateException("Unable to drop index '" + index + "' as table '" + table + "' does not exist");
      }
      return new RecordIterator(Collections.EMPTY_LIST, expression);      
   }
}
