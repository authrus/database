package com.authrus.database.terminal.command;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.authrus.database.Database;
import com.authrus.database.DatabaseConnection;
import com.authrus.database.Record;
import com.authrus.database.ResultIterator;
import com.authrus.database.Statement;
import com.authrus.database.sql.Query;
import com.authrus.database.sql.Verb;
import com.authrus.database.terminal.session.MemoryAnalyzer;
import com.authrus.database.terminal.session.SessionContext;

public class QueryExecutor {

   private final int limit;

   public QueryExecutor() {
      this(5000);
   }
   
   public QueryExecutor(int limit) {
      this.limit = limit;
   }
 
   public QueryResult execute(SessionContext session, QueryRequest request) throws Exception {
      ResultFormatter extractor = new ResultFormatter(session, request);
      List<String> titles = new ArrayList<String>();
      List<Record> results = new ArrayList<Record>();
      List<List<String>> rows = new ArrayList<List<String>>();
      
      try {        
         Query query = request.getQuery();
         Verb verb = query.getVerb();
         String source = query.getSource();
         Database database = session.getDatabase();
         DatabaseConnection connection = database.getConnection();
         MemoryAnalyzer analyzer = session.getAnalyzer();
         long start = System.currentTimeMillis();
         int repeat = request.getRepeat();
         
         if(verb == Verb.SELECT || verb == Verb.SELECT_DISTINCT) {
            analyzer.start();
            
            try {
               Statement statement = connection.prepareStatement(source);
               ResultIterator<Record> iterator = statement.execute();
            
               while (iterator.hasMore()) {
                  Record record = iterator.next();
                  results.add(record);
               }
            } finally {
               analyzer.stop();
            }
            if (!results.isEmpty()) {
               int count = results.size();
               
               for (int i = 0 ; i < count; i++) {
                  Record record = results.get(i);
                  Set<String> keys = record.getColumns();
                  Map<String, String> values = extractor.format(record);

                  if(i == 0) {
                     titles.addAll(keys);
                  }
                  List<String> cells = new ArrayList<String>();
                  
                  for(String title : titles) {
                     String value = values.get(title);
                     cells.add(value);
                  }
                  if (i > limit) { // don't allow it to go crazy!!
                     break;
                  }
                  rows.add(cells);
               }
            }
         } else {
            analyzer.start();
            
            try {
               connection.executeStatement(source);
            } finally {
               analyzer.stop();
            }            
         }
         if (repeat > 1) {
            for (int i = 1; i < repeat; i++) {
               connection.executeStatement(source);
            }
         }
         String memory = analyzer.change();
         long finish = System.currentTimeMillis();
         long duration = finish - start;
         
         return QueryResult.builder()
               .columns(titles)
               .rows(rows)
               .memory(memory)
               .duration(duration)
               .build();
      } catch (Exception e) {
         throw new IllegalStateException("Could not execute " + request, e);
      }
   }

}
