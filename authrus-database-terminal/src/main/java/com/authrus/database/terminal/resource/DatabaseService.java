package com.authrus.database.terminal.resource;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import com.authrus.database.Database;
import com.authrus.database.engine.Catalog;
import com.authrus.database.terminal.command.QueryResult;
import com.authrus.database.terminal.command.QueryRunner;
import com.authrus.database.terminal.session.MemoryAnalyzer;
import com.authrus.database.terminal.session.SessionContext;
import com.authrus.database.terminal.session.SessionTime;

@Slf4j
@AllArgsConstructor
public class DatabaseService {

   private final MemoryAnalyzer analyzer;
   private final SessionContext context;
   private final QueryRunner runner;
   private final SessionTime time;
   
   public DatabaseService(Database database, Catalog catalog){
      this.time = new SessionTime(null, null, -1);
      this.analyzer = new MemoryAnalyzer(); // creates thread locals
      this.context = new SessionContext(time, analyzer, database, catalog);
      this.runner = new QueryRunner();
   }
   
   @SneakyThrows
   public QueryResult select(DatabaseRequest request) {
      StringBuilder builder = new StringBuilder();
      
      Map<String, String> columns = request.getValues();      
      Set<String> names = columns.keySet();
      String schema = names.stream().collect(Collectors.joining(", "));
      String table = request.getTable();
      
      builder.append("select ");
      
      if(names.isEmpty()) {
         builder.append("*");
      } else {
         builder.append(schema);
      }
      builder.append(" from ");
      builder.append(table);
      builder.append(" where ");

      for(String name : names) {
         String value = columns.get(name);
         
         builder.append(name);
         builder.append(" == ");
         builder.append("'");
         builder.append(value);
         builder.append("'");
      }      
      String expression = builder.toString();
      
      log.info(expression);      
      return runner.run(context, expression);
   }
   
   @SneakyThrows
   public void create(DatabaseRequest request) {
      StringBuilder builder = new StringBuilder();      
      
      Map<String, String> columns = request.getValues();
      Set<String> names = columns.keySet();
      String table = request.getTable();
      
      builder.append("create table if not exists");
      builder.append(table);
      builder.append("(");
      builder.append("id int not null default sequence");
      
      for(String name : names) {
         String type = columns.get(name);
         
         builder.append(", ");
         builder.append(name);
         builder.append(" ");
         builder.append(type);
      }
      builder.append(", ");
      builder.append("created date default time");
      builder.append(", ");
      builder.append("primary key (id)");
      
      String expression = builder.toString();
      
      log.info(expression);
      runner.run(context, expression);
   }
   
   @SneakyThrows
   public void insert(DatabaseRequest request) {
      StringBuilder builder = new StringBuilder();    

      String table = request.getTable();
      Map<String, String> columns = request.getValues();
      Set<String> names = columns.keySet();
      String schema = names.stream().collect(Collectors.joining(", "));
      String values = names.stream()
            .map(name -> columns.get(name))
            .map(value -> String.format("'%s'", value))
            .collect(Collectors.joining(", "));
      
      builder.append("insert into ");
      builder.append(table);
      builder.append(" (");                      
      builder.append(schema);
      builder.append(") values (");
      builder.append(values);
      builder.append(")");

      String expression = builder.toString();
      
      log.info(expression);
      runner.run(context, expression);
   } 
}
