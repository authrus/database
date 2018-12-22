package com.authrus.database.engine.statement;

import java.util.Collections;

import com.authrus.database.Record;
import com.authrus.database.RecordIterator;
import com.authrus.database.ResultIterator;
import com.authrus.database.engine.Catalog;
import com.authrus.database.sql.Query;

public class CommitExecutor extends StatementExecutor {
   
   private final Catalog catalog;
   private final Query query;
   private final String origin;
   
   public CommitExecutor(Catalog catalog, Query query, String origin) {
      this.catalog = catalog;
      this.query = query;
      this.origin = origin;
   }
   
   @Override
   public ResultIterator<Record> execute() throws Exception {
      boolean disposed = closed.get();
      
      if(disposed) {
         throw new IllegalStateException("This statement has been closed");
      }
      String table = query.getTable();
      String expression = query.getSource();

      if(table == null) {
         throw new IllegalStateException("Transaction table missing from '" + expression + "'");
      }    
      catalog.commitTransaction(origin, table);

      return new RecordIterator(Collections.EMPTY_LIST, expression);      
   }
}
