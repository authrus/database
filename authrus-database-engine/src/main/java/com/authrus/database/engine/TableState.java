package com.authrus.database.engine;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.authrus.database.engine.index.RowCompressor;

public class TableState {
   
   private final Map<String, Row> previous;
   private final Map<String, Row> current;
   private final RowCompressor compressor;
   private final AtomicInteger changes;
   private final String name; 

   public TableState(String name) {
      this(name, 10000);
   }
   
   public TableState(String name, int capacity) {
      this.previous = new LinkedHashMap<String, Row>();
      this.current = new LinkedHashMap<String, Row>();
      this.compressor = new RowCompressor(capacity);
      this.changes = new AtomicInteger();
      this.name = name;
   } 
   
   public Set<String> keys() {
      return current.keySet();
   }
   
   public Collection<Row> values() {
      return current.values();
   }
   
   public boolean isEmpty() {
      return current.isEmpty();
   }
   
   public Row get(String key) {
      return current.get(key);
   }
   
   public Row insert(String key, Row tuple) {
      Row previous = current.put(key, tuple);
      
      if(previous != tuple) {
         Row replace = compressor.compress(tuple); // intern strings
         
         if(tuple != replace) { // was compressed
            current.put(key, replace);
         }
         changes.getAndIncrement();
      }
      return previous;
   }   
   
   public Row remove(String key) {
      Row tuple = current.remove(key);
      
      if(tuple != null) {
         changes.getAndIncrement();
      }
      return tuple;
   }
   
   public void mark() {
      previous.clear();
      previous.putAll(current);
      changes.set(0);          
   }   
   
   public boolean revert() {
      int count = changes.get(); // avoid excessive rehashing
      
      if(count > 0) {
         current.clear();
         current.putAll(previous);
         previous.clear(); // do not hold tuples
         changes.set(0);
      }
      return count > 0;
   }
   
   public boolean save() {
      int count = changes.get();
            
      if(count > 0) {
         previous.clear();
         changes.set(0);
      }
      return count > 0; // did anything change
   } 
   
   public void clear() {
      changes.getAndIncrement();
      current.clear();
   }
   
   public int size() {
      return current.size();
   }
   
   @Override
   public String toString() {
      return name;
   }
}

