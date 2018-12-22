package com.authrus.database.engine;

import static com.authrus.database.engine.TransactionType.FULL;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.authrus.database.Schema;

public class Catalog {

   private final Map<String, Table> tables;
   private final TransactionManager manager;
   private final ChangeListener listener;
   private final TableBuilder builder;
   
   public Catalog(ChangeListener listener, String owner) { 
      this.tables = new LinkedHashMap<String, Table>();
      this.manager = new TransactionManager(tables, owner);
      this.builder = new TableBuilder(manager, listener, owner);
      this.listener = listener;
   }
   
   public synchronized Set<String> listTables() {
      return tables.keySet();
   }
   
   public synchronized Table findTable(String name) {
      return tables.get(name);
   }     
   
   public synchronized Transaction findTransaction(String name) {
      return manager.find(name);
   }     
   
   public synchronized Table createTable(String origin, String name, Schema schema) {
      Transaction transaction = manager.find(name);
      Table table = tables.get(name);
      
      if(table == null) {   
         table = builder.createTable(origin, name, schema);           
         
         if(transaction != null) {
            TransactionType type = transaction.getType();
            
            if(type.isPersistent()) {
               listener.onCreate(origin, name, schema); // record change from origin not owner!
            }
         } else {
            listener.onCreate(origin, name, schema); // record change from origin not owner!       
         }     
         tables.put(name, table);
      }
      return table;
   }
   
   public synchronized Table dropTable(String origin, String name) {
      Transaction transaction = manager.find(name);
      Table table = tables.remove(name);
      
      if(table != null) {
         if(transaction != null) {
            TransactionType type = transaction.getType();
            
            if(type.isPersistent()) {
               listener.onDrop(origin, name);
            }
         } else {
            listener.onDrop(origin, name);         
         }
      }
      return table;
   }
   
   public synchronized Table saveTable(String origin, String name) {
      Transaction transaction = manager.find(name);
      Table table = tables.get(name);
      
      if(table != null) {
         TableModel model = table.getModel();         
         Schema schema = table.getSchema();
         List<Row> tuples = model.list();         
         
         if(transaction != null) {
            TransactionType type = transaction.getType();
            
            if(type.isPersistent()) {
               listener.onDrop(origin, name);
               listener.onCreate(origin, name, schema);
            
               for(Row tuple : tuples) {
                  listener.onInsert(origin, name, tuple);
               }                             
            }
         } else {
            listener.onDrop(origin, name);
            listener.onCreate(origin, name, schema);
         
            for(Row tuple : tuples) {
               listener.onInsert(origin, name, tuple);
            } 
         }
      }
      return table;
   }    
   
   public synchronized Transaction beginTransaction(String origin, String name, String token) {
      return beginTransaction(origin, name, token, FULL);
   }
   
   public synchronized Transaction beginTransaction(String origin, String name, String token, TransactionType type) {
      Transaction transaction = manager.begin(name, token, type);
      
      if(type.isPersistent()) {
         listener.onBegin(origin, name, transaction);
      }           
      return transaction;      
   }
   
   public synchronized Transaction commitTransaction(String origin, String name) {
      Transaction transaction = manager.commit(name);
      
      if(transaction != null) {
         TransactionType type = transaction.getType();
         
         if(type.isPersistent()) {
            listener.onCommit(origin, name);
         }
      }
      return transaction;
   }
   
   public synchronized Transaction rollbackTransaction(String origin, String name) {
      Transaction transaction = manager.rollback(name);
      
      if(transaction != null) {
         TransactionType type = transaction.getType();
         
         if(type.isPersistent()) {
            listener.onRollback(origin, name);
         }
      }
      return transaction;
   }
}
