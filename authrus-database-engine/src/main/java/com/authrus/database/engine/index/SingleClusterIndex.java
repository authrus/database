package com.authrus.database.engine.index;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import com.authrus.database.engine.Row;

public class SingleClusterIndex<T extends Comparable<T>> implements ColumnIndex<T> {
   
   private final Set<Row> blank;
   private final Cluster cluster;

   public SingleClusterIndex() {
      this(null);
   }
   
   public SingleClusterIndex(Cluster cluster) {
      this.blank = Collections.emptySet();
      this.cluster = cluster;
   }

   @Override
   public ColumnIndex<T> greaterThan(T value) {
      return new SingleClusterIndex<T>();
   }

   @Override
   public ColumnIndex<T> lessThan(T value) {
      return new SingleClusterIndex<T>();
   }

   @Override
   public ColumnIndex<T> greaterThanOrEqual(T value) {
      return this;
   }

   @Override
   public ColumnIndex<T> lessThanOrEqual(T value) {
      return this;
   }

   @Override
   public ColumnIndex<T> equalTo(T value) {
      return this;
   }

   @Override
   public ColumnIndex<T> notEqualTo(T value) {
      return new SingleClusterIndex<T>();
   }     
   
   @Override
   public Iterator<Row> iterator() {
      if(cluster != null) {
         return cluster.iterator();
      }
      return blank.iterator();
   }

   @Override
   public int count() {
      if(cluster != null) {
         return cluster.size();
      }
      return blank.size();
   }   
}
