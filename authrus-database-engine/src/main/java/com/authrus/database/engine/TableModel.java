package com.authrus.database.engine;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.authrus.database.Column;
import com.authrus.database.PrimaryKey;
import com.authrus.database.engine.filter.Filter;

public class TableModel {

   private final ReadWriteLock lock;
   private final TableEngine engine;

   public TableModel(TransactionManager manager, ChangeListener listener, PrimaryKey key, String owner, String name) {
      this(manager, listener, key, owner, name, 1000000);
   }
   
   public TableModel(TransactionManager manager, ChangeListener listener, PrimaryKey key, String owner, String name, int capacity) {
      this.engine = new TableEngine(manager, listener, key, owner, name, capacity);
      this.lock = new ReentrantReadWriteLock();
   }      
   
   public Row get(String key) {
      Lock read = lock.readLock();
      
      try {
         read.lock();      
         return engine.get(key);
      } finally {
         read.unlock();
      }
   }      
   
   public Row remove(String key) {
      Lock write = lock.writeLock();
      
      try {
         write.lock();      
         return engine.remove(key);
      } finally {
         write.unlock();
      }      
   }
   
   public List<Row> remove(Filter filter) {
      Lock write = lock.writeLock();
      
      try {
         write.lock();      
         return engine.remove(filter);
      } finally {
         write.unlock();
      } 
   }
   
   public Row insert(Row tuple) {
      Lock write = lock.writeLock();
      
      try {
         write.lock();      
         return engine.insert(tuple);
      } finally {
         write.unlock();
      } 
   }
   
   public List<Row> list() {
      Lock read = lock.readLock();
      
      try {
         read.lock();      
         return engine.list();
      } finally {
         read.unlock();
      }
   }
   
   public int count(Filter filter) {
      Lock read = lock.readLock();
      
      try {
         read.lock();      
         return engine.count(filter);
      } finally {
         read.unlock();
      }
   }
   
   public List<Row> list(Filter filter) {
      Lock read = lock.readLock();
      
      try {
         read.lock();      
         return engine.list(filter);
      } finally {
         read.unlock();
      }      
   }   
   
   public boolean revert() {
      Lock write = lock.writeLock();
      
      try {
         write.lock();      
         return engine.revert();
      } finally {
         write.unlock();
      } 
   }       
   
   public boolean mark() {
      Lock write = lock.writeLock();
      
      try {
         write.lock();      
         return engine.mark();
      } finally {
         write.unlock();
      } 
   }
   
   public boolean save() {
      Lock write = lock.writeLock();
      
      try {
         write.lock();      
         return engine.save();
      } finally {
         write.unlock();
      } 
   }    
   
   public void index(Column column) {
      Lock write = lock.writeLock();
      
      try {
         write.lock();      
         engine.index(column);
      } finally {
         write.unlock();
      } 
   }    
   
   public void clear() {
      Lock write = lock.writeLock();
      
      try {
         write.lock();      
         engine.clear();
      } finally {
         write.unlock();
      } 
   }
   
   public int size() {
      Lock read = lock.readLock();
      
      try {
         read.lock();      
         return engine.size();
      } finally {
         read.unlock();
      }
   }
}
