package com.authrus.database.engine;

import static com.authrus.database.engine.TransactionType.FULL;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TransactionManager {
   
   private final ConcurrentMap<String, Transaction> transactions;
   private final Map<String, Table> tables;
   private final String owner;

   public TransactionManager(Map<String, Table> tables, String owner) {     
      this.transactions = new ConcurrentHashMap<String, Transaction>();
      this.tables = tables;
      this.owner = owner;
   }
   
   public synchronized Transaction find(String table) {
      return transactions.get(table);
   }   
   
   public synchronized Transaction begin(String table, String token) {
      return begin(table, token, FULL);
   }

   public synchronized Transaction begin(String name, String token, TransactionType type) {
      Transaction transaction = create(name, token, type);
      Table table = tables.get(name);

      if(transactions.putIfAbsent(name, transaction) != null) {
         throw new IllegalStateException("Transaction for '" + name + "' was not committed");
      }
      if(table != null) {
         TableModel model = table.getModel();
         
         if(type.isAtomic()) {
            model.mark();
         }
      }
      return transaction;
   } 
   
   public synchronized Transaction commit(String name) {
      Transaction transaction = transactions.get(name);
      Table table = tables.get(name);
      
      if(transaction == null) {
         throw new IllegalStateException("Transaction for '" + name + "' does not exist");
      }
      if(table != null) {
         TransactionType type = transaction.getType();
         TableModel model = table.getModel();
         
         if(type.isAtomic()) {
            model.save();
         }
      }
      transactions.remove(name);
      return transaction;
   }
   
   public synchronized Transaction rollback(String name) {
      Transaction transaction = transactions.get(name);
      Table table = tables.get(name);
      
      if(transaction != null && table != null) {
         TransactionType type = transaction.getType();
         TableModel model = table.getModel();
         
         if(type.isAtomic()) {
            model.revert();
         }
      }
      transactions.remove(name);
      return transaction;
   }      
   
   private synchronized Transaction create(String table, String token, TransactionType type) {
      int index = token.indexOf('@');
      int length = token.length();
      
      if(length <= 0) {
         throw new IllegalArgumentException("Transaction for '" + table+ " requires a name");
      }      
      if(index != -1) {
         return new ExternalTransaction(table, token, type);
      }
      return new LocalTransaction(table, token, type);
   } 
   
   private class ExternalTransaction implements Transaction {
      
      private final TransactionParser parser;
      private final TransactionType type;
      private final String table;
      private final String token;

      public ExternalTransaction(String table, String token, TransactionType type){
         this.parser = new TransactionParser(token);              
         this.table = table;
         this.token = token;
         this.type = type;
      }
      
      @Override
      public TransactionType getType() {
         return type;
      }     
      
      @Override
      public String getTable() {
         return table;
      }       
      
      @Override
      public String getName() {
         return parser.getName();
      }
      
      @Override
      public String getToken() {
         return token;
      }       
      
      @Override
      public String getOrigin() {
         return parser.getOrigin();
      }            

      @Override
      public Long getSequence() {
         return parser.getSequence();
      }

      @Override
      public Long getTime() {
         return parser.getTime();
      }
      
      @Override
      public String toString(){
         return token;
      }
   }   
   
   private class LocalTransaction implements Transaction {      
    
      private final TransactionType type;
      private final String table;
      private final String token;    

      public LocalTransaction(String table, String token, TransactionType type){        
         this.table = table;
         this.token = token;
         this.type = type;
      }
      
      @Override
      public TransactionType getType() {
         return type;
      }
      
      @Override
      public String getTable() {
         return table;
      }
      
      @Override
      public String getName() {
         return token;
      }
      
      @Override
      public String getToken() {
         return token;
      }       
      
      @Override
      public String getOrigin() {
         return owner;
      }

      @Override
      public Long getSequence() {
         return null;
      }

      @Override
      public Long getTime() {
         return null;
      }
      
      @Override
      public String toString(){
         return token;
      }
   }   
}
