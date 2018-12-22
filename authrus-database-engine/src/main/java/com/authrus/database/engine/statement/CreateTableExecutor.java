package com.authrus.database.engine.statement;

import java.util.Collections;

import com.authrus.database.Record;
import com.authrus.database.RecordIterator;
import com.authrus.database.ResultIterator;
import com.authrus.database.Schema;
import com.authrus.database.engine.Catalog;
import com.authrus.database.engine.Table;
import com.authrus.database.sql.Query;

public class CreateTableExecutor extends StatementExecutor {
   
   private final Catalog catalog;
   private final Query query;
   private final String origin;
   
   public CreateTableExecutor(Catalog catalog, Query query, String origin) {
      this.catalog = catalog;
      this.origin = origin;
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
      Table table = catalog.findTable(name);
      
      if(table == null) {
         Schema schema = query.getCreateSchema();
         
         if(schema == null) {
            throw new IllegalStateException("No schema dpecified for '" + expression + "'");
         }
         catalog.createTable(origin, name, schema);
      }
      return new RecordIterator(Collections.EMPTY_LIST, expression);      
   }
}
