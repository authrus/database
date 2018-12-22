package com.authrus.database.engine.index;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import com.authrus.database.engine.Cell;
import com.authrus.database.engine.Row;

public class DuplicateCluster implements Cluster {
   
   private final Map<String, Row> tuples;
   private final String name;
   private final int index;
   
   public DuplicateCluster(String name, int index) {
      this.tuples = new LinkedHashMap<String, Row>();
      this.name = name;
      this.index = index;
   }   

   @Override
   public Iterator<Row> iterator() {
      return tuples.values().iterator();
   }   

   @Override
   public void insert(String key, Row tuple) {
      Cell cell = tuple.getCell(index);
      
      if(cell == null) {
         throw new IllegalArgumentException("Index column '" + name + "' missing");
      }
      tuples.put(key, tuple);
   }
   
   @Override
   public void remove(String key) {
      tuples.remove(key);
   }
  
   @Override
   public void clear() {
      tuples.clear();
   }
   
   @Override
   public int size() {
      return tuples.size();
   }
}
