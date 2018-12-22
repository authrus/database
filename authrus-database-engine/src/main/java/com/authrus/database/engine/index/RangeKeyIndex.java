package com.authrus.database.engine.index;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Set;

import com.authrus.database.engine.Row;

public class RangeKeyIndex<T extends Comparable<T>> implements ColumnIndex<T> {

   private final NavigableMap<T, Row> tuples;
   private final Set<Row> exclude;
   private final Set<Row> blank;

   public RangeKeyIndex(NavigableMap<T, Row> tuples) {
      this(tuples, Collections.EMPTY_SET);
   }
   
   public RangeKeyIndex(NavigableMap<T, Row> tuples, Set<Row> exclude) {
      this.blank = Collections.emptySet();
      this.exclude = exclude;
      this.tuples = tuples;
   }

   @Override
   public ColumnIndex<T> greaterThan(T value) {
      NavigableMap<T, Row> matches = tuples.tailMap(value, false);

      if (!matches.isEmpty()) {
         return new RangeKeyIndex<T>(matches, exclude);
      }
      return new SingleKeyIndex<T>();
   }

   @Override
   public ColumnIndex<T> lessThan(T value) {
      NavigableMap<T, Row> matches = tuples.headMap(value, false);

      if (!matches.isEmpty()) {
         return new RangeKeyIndex<T>(matches, exclude);
      }
      return new SingleKeyIndex<T>();
   }

   @Override
   public ColumnIndex<T> greaterThanOrEqual(T value) {
      NavigableMap<T, Row> matches = tuples.tailMap(value, true);

      if (!matches.isEmpty()) {
         return new RangeKeyIndex<T>(matches, exclude);
      }
      return new SingleKeyIndex<T>();
   }

   @Override
   public ColumnIndex<T> lessThanOrEqual(T value) {
      NavigableMap<T, Row> matches = tuples.headMap(value, true);

      if (!matches.isEmpty()) {
         return new RangeKeyIndex<T>(matches, exclude);
      }
      return new SingleKeyIndex<T>();
   }

   @Override
   public ColumnIndex<T> equalTo(T value) {
      Row tuple = tuples.get(value);

      if (tuple == null) {
         return new SingleKeyIndex<T>();
      }
      if(exclude.contains(value)) {
         return new SingleKeyIndex<T>();
      }
      return new SingleKeyIndex<T>(tuple);

   }

   @Override
   public ColumnIndex<T> notEqualTo(T value) {
      Row tuple = tuples.get(value);

      if (tuple != null) {
         Set<Row> keys = new HashSet<Row>();         
         
         keys.addAll(exclude);
         keys.add(tuple);

         return new RangeKeyIndex<T>(tuples, exclude);
      }
      return this;
   }

   @Override
   public Iterator<Row> iterator() {
      Collection<Row> list = tuples.values();
      
      if(!list.isEmpty()) {
         return list.iterator();
      }
      return blank.iterator();
   }
   
   @Override
   public int count() {
      return tuples.size() - exclude.size();
   } 
}
