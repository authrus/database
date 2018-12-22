package com.authrus.database.engine.index;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import com.authrus.database.engine.Cell;
import com.authrus.database.engine.Row;

public class UniqueCluster implements Cluster {

   private Collection<Row> reference;
   private Collection<Row> empty;
   private String require;
   private Map binding;
   private int index;
   
   public UniqueCluster(Map binding, String require, int index) {
      this.reference = Collections.emptyList();
      this.require = require;
      this.empty = reference;
      this.binding = binding;
      this.index = index;
   }   

   @Override
   public Iterator<Row> iterator() {
      return reference.iterator();
   }   

   @Override
   public void insert(String key, Row tuple) {
      Cell cell = tuple.getCell(index);
      Comparable value = cell.getValue();
        
      if(!require.equals(key)) {
         throw new IllegalArgumentException("Invalid key '" + require + "' is not equal to '" + key + "'");
      }
      if(binding.containsKey(value)) { // table wide bindings
         Object current = binding.get(value);
         
         if(!key.equals(current)) {
            throw new IllegalArgumentException("Value at " + index + " for '" + key + "' is a duplicate of '" + current + "'");
         }
      } else {
         binding.put(value, key);
      }
      reference = Collections.singleton(tuple);     
   }
   
   @Override
   public void remove(String key) {
      Iterator<Row> iterator = reference.iterator();
      
      if(!require.equals(key)) {
         throw new IllegalArgumentException("Invalid key '" + require + "' is not equal to '" + key + "'");
      }
      if(iterator.hasNext()) {
         Row tuple = iterator.next();
         Cell cell = tuple.getCell(index);
         Comparable value = cell.getValue();
         
         binding.remove(value);
      }
      reference = empty;
   }   
   
   @Override
   public void clear() {
      Iterator<Row> iterator = reference.iterator();
      
      if(iterator.hasNext()) {
         Row tuple = iterator.next();
         Cell cell = tuple.getCell(index);
         Comparable value = cell.getValue();
         
         binding.remove(value);
      }
      reference = empty;
   }   
   
   @Override
   public int size() {
      return reference.size();
   } 
}
