package com.authrus.database.engine.index;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Set;

import com.authrus.database.engine.Row;

public class RangeClusterIndex<T extends Comparable<T>> implements ColumnIndex<T> {

   private final NavigableMap<T, Cluster> clusters;
   private final Set<Cluster> exclude;

   public RangeClusterIndex(NavigableMap<T, Cluster> clusters) {
      this(clusters, Collections.EMPTY_SET);
   }
   
   public RangeClusterIndex(NavigableMap<T, Cluster> clusters, Set<Cluster> exclude) {
      this.exclude = exclude;
      this.clusters = clusters;
   }

   @Override
   public ColumnIndex<T> greaterThan(T value) {
      NavigableMap<T, Cluster> matches = clusters.tailMap(value, false);

      if (!matches.isEmpty()) {
         return new RangeClusterIndex<T>(matches, exclude);
      }
      return new SingleClusterIndex<T>();
   }

   @Override
   public ColumnIndex<T> lessThan(T value) {
      NavigableMap<T, Cluster> matches = clusters.headMap(value, false);

      if (!matches.isEmpty()) {
         return new RangeClusterIndex<T>(matches, exclude);
      }
      return new SingleClusterIndex<T>();
   }

   @Override
   public ColumnIndex<T> greaterThanOrEqual(T value) {
      NavigableMap<T, Cluster> matches = clusters.tailMap(value, true);

      if (!matches.isEmpty()) {
         return new RangeClusterIndex<T>(matches, exclude);
      }
      return new SingleClusterIndex<T>();
   }

   @Override
   public ColumnIndex<T> lessThanOrEqual(T value) {
      NavigableMap<T, Cluster> matches = clusters.headMap(value, true);

      if (!matches.isEmpty()) {
         return new RangeClusterIndex<T>(matches, exclude);
      }
      return new SingleClusterIndex<T>();
   }

   @Override
   public ColumnIndex<T> equalTo(T value) {
      Cluster cluster = clusters.get(value);

      if (cluster == null) {
         return new SingleClusterIndex<T>();
      }
      if(exclude.contains(value)) {
         return new SingleClusterIndex<T>();
      }
      return new SingleClusterIndex<T>(cluster);

   }

   @Override
   public ColumnIndex<T> notEqualTo(T value) {
      Cluster cluster = clusters.get(value);

      if (cluster != null) {
         Set<Cluster> keys = new HashSet<Cluster>();         
         
         keys.addAll(exclude);
         keys.add(cluster);

         return new RangeClusterIndex<T>(clusters, exclude);
      }
      return this;
   }

   @Override
   public Iterator<Row> iterator() {
      Collection<Cluster> list = clusters.values();
      Iterator<Cluster> iterator = list.iterator();

      return new ClusterIterator(iterator, exclude);
   }
   
   @Override
   public int count() {
      Iterable<Cluster> groups = clusters.values();
      int count = 0;
      
      for(Cluster group : groups){
         if(!exclude.contains(group)) {
            count += group.size();
         }
      }
      return count;
   }   
   
}
