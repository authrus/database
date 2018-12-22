package com.authrus.database.engine.index;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import com.authrus.database.Column;
import com.authrus.database.engine.Cell;
import com.authrus.database.engine.Row;

public class ClusterIndex<T extends Comparable<T>> implements ColumnIndexUpdater<T> {
   
   private final NavigableMap<T, Cluster> clusters;
   private final ClusterBuilder builder;
   private final Column column;
   private final String name;
   private final int index;
   
   public ClusterIndex(ClusterBuilder builder, Column column) {
      this.clusters = new TreeMap<T, Cluster>();
      this.index = column.getIndex();
      this.name = column.getName();
      this.builder = builder;
      this.column = column;
   }

   @Override
   public ColumnIndex<T> greaterThan(T value) {
      if(value == null) {
         throw new IllegalStateException("Comparing index column '" + name + "' with null");
      }         
      NavigableMap<T, Cluster> matches = clusters.tailMap(value, false);

      if (!matches.isEmpty()) {
         return new RangeClusterIndex<T>(matches);
      }
      return new SingleClusterIndex<T>();
   }

   @Override
   public ColumnIndex<T> lessThan(T value) {
      if(value == null) {
         throw new IllegalStateException("Comparing index column '" + name + "' with null");
      }           
      NavigableMap<T, Cluster> matches = clusters.headMap(value, false);

      if (!matches.isEmpty()) {
         return new RangeClusterIndex<T>(matches);
      }
      return new SingleClusterIndex<T>();
   }

   @Override
   public ColumnIndex<T> greaterThanOrEqual(T value) {
      if(value == null) {
         throw new IllegalStateException("Comparing index column '" + name + "' with null");
      }            
      NavigableMap<T, Cluster> matches = clusters.tailMap(value, true);

      if (!matches.isEmpty()) {
         return new RangeClusterIndex<T>(matches);
      }
      return new SingleClusterIndex<T>();
   }

   @Override
   public ColumnIndex<T> lessThanOrEqual(T value) {
      if(value == null) {
         throw new IllegalStateException("Comparing index column '" + name + "' with null");
      }            
      NavigableMap<T, Cluster> matches = clusters.headMap(value, true);

      if (!matches.isEmpty()) {
         return new RangeClusterIndex<T>(matches);
      }
      return new SingleClusterIndex<T>();
   }

   @Override
   public ColumnIndex<T> equalTo(T value) {
      if(value == null) {
         throw new IllegalStateException("Comparing index column '" + name + "' with null");
      }           
      Cluster cluster = clusters.get(value);

      if (cluster != null) {
         return new SingleClusterIndex<T>(cluster);
      }
      return new SingleClusterIndex<T>();      
   }

   @Override
   public ColumnIndex<T> notEqualTo(T value) {
      if(value == null) {
         throw new IllegalStateException("Comparing index column '" + name + "' with null");
      }         
      Set<Cluster> exclude = Collections.emptySet();
      Cluster cluster = clusters.get(value);

      if (cluster != null) {
         exclude = Collections.singleton(cluster);
      }
      return new RangeClusterIndex<T>(clusters, exclude);
   }  
   
   @Override
   public void update(Row tuple) {
      Cell cell = tuple.getCell(index);
      T value = (T)cell.getValue();
      
      if(value == null) {
         throw new IllegalStateException("Value for column '" + name + "' is null");
      }
      String key = tuple.getKey();
      Cluster cluster = clusters.get(value);
      
      if(cluster == null) {
         cluster = builder.create(key, column);
         clusters.put(value, cluster);
      }
      cluster.insert(key, tuple);      
   }   

   @Override
   public void remove(Row tuple) {
      Cell cell = tuple.getCell(index);
      T value = (T)cell.getValue();
      
      if(value == null) {
         throw new IllegalStateException("Value for column '" + name + "' is null");
      }
      String key = tuple.getKey();
      Cluster cluster = clusters.get(value);
      
      if(cluster != null) {
         cluster.remove(key);
      } 
   }

   @Override
   public Iterator<Row> iterator() {
      Collection<Cluster> list = clusters.values();
      Iterator<Cluster> iterator = list.iterator();

      return new ClusterIterator(iterator);
   }    
   
   @Override
   public int count() {
      Iterable<Cluster> groups = clusters.values();
      int count = 0;
      
      for(Cluster group : groups){
         count += group.size();
      }
      return count;
   }

   @Override
   public void clear() {
      clusters.clear();
   }
}
