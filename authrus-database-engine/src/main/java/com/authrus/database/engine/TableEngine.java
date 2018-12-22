package com.authrus.database.engine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.authrus.database.Column;
import com.authrus.database.PrimaryKey;
import com.authrus.database.engine.filter.Filter;
import com.authrus.database.engine.index.TableIndexer;

class TableEngine {

   private final AtomicReference<String> point;
   private final TransactionManager manager;
   private final ChangeListener listener;
   private final TableIndexer indexer;
   private final TableState state;
   private final PrimaryKey key;
   private final String owner;
   private final String name; 

   public TableEngine(TransactionManager manager, ChangeListener listener, PrimaryKey key, String owner, String name) {
      this(manager, listener, key, owner, name, 1000000);
   }
   
   public TableEngine(TransactionManager manager, ChangeListener listener, PrimaryKey key, String owner, String name, int capacity) {
      this.state = new TableState(name);      
      this.indexer = new TableIndexer(key, state);
      this.point = new AtomicReference<String>();
      this.listener = listener;
      this.manager = manager;
      this.owner = owner;
      this.name = name;
      this.key = key;
   }      
   
   public Row get(String key) {
      return state.get(key);
   }      
   
   public Row remove(String key) {
      Row tuple = indexer.remove(key);
      Transaction transaction = manager.find(name);
      
      if(tuple != null) {
         if(transaction != null) {
            TransactionType type = transaction.getType();
            String origin = transaction.getOrigin();
            
            if(type.isPersistent()) {
               listener.onDelete(origin, name, key);
            }
         } else {
            listener.onDelete(owner, name, key);
         }
      }
      return tuple;
   }
   
   public List<Row> remove(Filter filter) {
      List<Row> result = list(filter);
      
      for(Row tuple : result) {
         String key = tuple.getKey();
         
         if(key != null) {
            remove(key);
         }
      }
      return result;
   }
   
   public Row insert(Row tuple) {
      String key = tuple.getKey();
      Row previous = indexer.insert(key, tuple);
      Transaction transaction = manager.find(name);
      
      if(transaction != null) {
         TransactionType type = transaction.getType();
         String origin = transaction.getOrigin();
         
         if(type.isPersistent()) {
            if(previous != null) {
               listener.onUpdate(origin, name, tuple, previous);
            } else {
               listener.onInsert(origin, name, tuple);
            }
         }
      } else {
         if(previous != null) {
            listener.onUpdate(owner, name, tuple, previous);
         } else {
            listener.onInsert(owner, name, tuple);
         }
      }      
      return previous;
   }
   
   public List<Row> list() {
      List<Row> result = new ArrayList<Row>();
      
      if(!state.isEmpty()) {
         Iterable<Row> list = state.values();
         
         for(Row tuple : list) {
            if(tuple != null) {
               result.add(tuple);
            }         
         }
      }
      return result;
   }
   
   public int count(Filter filter) {
      int size = state.size();
      
      if(size > 0) {
         return indexer.count(filter);
      }
      return 0;
   }
   
   public List<Row> list(Filter filter) {
      List<Row> result = new ArrayList<Row>();

      if(!state.isEmpty()) {
         Iterator<Row> tuples = indexer.select(filter);
         
         while(tuples.hasNext()) {
            Row tuple = tuples.next();
            
            if(tuple != null) {
               result.add(tuple);
            }          
         }
      }
      return result;
   }   
   
   public boolean revert() {
      Transaction transaction = manager.find(name);
      
      if(transaction != null) {  
         String name = transaction.getToken();
         
         if(point.compareAndSet(name, null)) {
            if(state.revert() && indexer.clear()) {
               Collection<Row> tuples = state.values();
               
               for(Row tuple : tuples) { // index again
                  String key = tuple.getKey();
                  
                  if(key != null) {
                     indexer.insert(key, tuple);
                  }
               }                             
            }
            return true;
         }
      }
      return false;
   }       
   
   public boolean mark() {
      Transaction transaction = manager.find(name);
      
      if(transaction != null) {
         String name = transaction.getToken();
         
         if(point.compareAndSet(null, name)) {            
            state.mark();
            return true;
         }
      }
      return false;
   }
   
   public boolean save() {
      Transaction transaction = manager.find(name);
      
      if(transaction != null) {
         String name = transaction.getToken();
         
         if(point.compareAndSet(name, null)) {            
            state.save();
            return true;
         }
      }
      return false;
   }    
   
   public void index(Column column) {
      String index = column.getName();
      Transaction transaction = manager.find(name);      
      List<String> keys = key.getColumns();
      
      if(!keys.contains(index)) { 
         if(transaction != null) {            
            TransactionType type = transaction.getType();
            String origin = transaction.getOrigin();
            
            if(type.isPersistent()) {
               listener.onIndex(origin, name, index);
            }
         } else {
            listener.onIndex(owner, name, index);
         }  
      }
      indexer.index(column);
   }    
   
   public void clear() {
      Iterable<Row> list = state.values();
      Transaction transaction = manager.find(name);
      
      for(Row tuple : list) {
         String key = tuple.getKey();
         
         if(transaction != null) {
            TransactionType type = transaction.getType();
            String origin = transaction.getOrigin();
            
            if(type.isPersistent()) {
               listener.onDelete(origin, name, key);
            }
         } else {
            listener.onDelete(owner, name, key);
         }        
      }
      indexer.clear();
      state.clear();
   }
   
   public int size() {
      return state.size();
   }
}
