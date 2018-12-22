package com.authrus.database.engine.io.read;

import com.authrus.database.Column;
import com.authrus.database.Schema;
import com.authrus.database.engine.Catalog;
import com.authrus.database.engine.Row;
import com.authrus.database.engine.Table;
import com.authrus.database.engine.TableModel;
import com.authrus.database.engine.Transaction;
import com.authrus.database.engine.TransactionType;

public class CatalogAssembler implements ChangeAssembler {
   
   private final ChangeSetMerger merger;
   private final Catalog catalog;
   
   public CatalogAssembler(Catalog catalog) {
      this.merger = new ChangeSetMerger(catalog);
      this.catalog = catalog;
   }   

   @Override
   public void onBegin(String origin, String name, Transaction transaction) {
      Transaction abort = catalog.findTransaction(name);
      
      if(abort != null) {
         catalog.rollbackTransaction(origin, name); // this can be caused by an exception etc...
      }
      String token = transaction.getToken();
      TransactionType type = transaction.getType();
      
      catalog.beginTransaction(origin, name, token, type);      
   }
   
   @Override
   public void onCreate(String origin, String name, Schema schema) {
      Table table = catalog.findTable(name);
      
      if(table != null) {
         throw new IllegalStateException("Table '" + name + "' already exists");
      }
      catalog.createTable(origin, name, schema);
   }

   @Override
   public void onInsert(String origin, String name, ChangeSet change) {
      Row tuple = merger.insertState(name, change);
      Table table = catalog.findTable(name);
      TableModel model = table.getModel();
      
      if(model == null) {
         throw new IllegalStateException("No table found for '" + name + "'");
      }
      model.insert(tuple);
   }
   
   @Override
   public void onUpdate(String origin, String name, ChangeSet change) {
      Row tuple = merger.updateState(name, change);
      Table table = catalog.findTable(name);
      TableModel model = table.getModel();
      
      if(model == null) {
         throw new IllegalStateException("No table found for '" + name + "'");
      }
      model.insert(tuple);
   }   
   
   @Override
   public void onDelete(String origin, String name, String key) {
      Table table = catalog.findTable(name);
      TableModel model = table.getModel();
      
      if(model == null) {
         throw new IllegalStateException("No table found for '" + name + "'");
      }
      merger.deleteState(name, key);
      model.remove(key);
   }    

   @Override
   public void onIndex(String origin, String name, String index) {
      Table table = catalog.findTable(name);
      
      if(table == null) {
         throw new IllegalStateException("Table '" + name + "' does not exist");
      }
      TableModel model = table.getModel();
      Schema schema = table.getSchema();
      Column column = schema.getColumn(index);
      
      if(column == null) {
         throw new IllegalStateException("Column '" + index + "' does not exist in '" + name + "'");
      }
      model.index(column);
   }
   
   @Override
   public void onCommit(String origin, String name) {
      Transaction current = catalog.findTransaction(name);
      
      if(current == null) {
         throw new IllegalStateException("Transaction for '" + name + "' does not exists");
      }
      catalog.commitTransaction(origin, name); 
   }   

   @Override
   public void onRollback(String origin, String name) {
      Transaction current = catalog.findTransaction(name);
      
      if(current == null) {
         throw new IllegalStateException("Transaction for '" + name + "' does not exists");
      }
      catalog.rollbackTransaction(origin, name); 
   }   
   
   @Override
   public void onDrop(String origin, String name) {
      Table table = catalog.dropTable(origin, name);
      
      if(table != null) {
         merger.dropState(name); 
      }    
   }
}
