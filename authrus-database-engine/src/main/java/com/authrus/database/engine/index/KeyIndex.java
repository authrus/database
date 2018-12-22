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

public class KeyIndex<T extends Comparable<T>> implements ColumnIndexUpdater<T> {   

   private final NavigableMap<T, Row> tuples;
   private final Set<Row> blank;
   private final String name;
   private final int index;
   
   public KeyIndex(Column column) {
      this.tuples = new TreeMap<T, Row>();
      this.blank = Collections.emptySet();
      this.index = column.getIndex();
      this.name = column.getName();       
   }

   @Override
   public ColumnIndex<T> greaterThan(T value) {  
      if(value == null) {
         throw new IllegalStateException("Comparing key '" + name + "' with null");
      }      
      NavigableMap<T, Row> matches = tuples.tailMap(value, false);

      if (!matches.isEmpty()) {
         return new RangeKeyIndex<T>(matches);
      }
      return new SingleKeyIndex<T>();
   }

   @Override
   public ColumnIndex<T> lessThan(T value) {
      if(value == null) {
         throw new IllegalStateException("Comparing key '" + name + "' with null");
      }   
      NavigableMap<T, Row> matches = tuples.headMap(value, false);

      if (!matches.isEmpty()) {
         return new RangeKeyIndex<T>(matches);
      }
      return new SingleKeyIndex<T>();
   }

   @Override
   public ColumnIndex<T> greaterThanOrEqual(T value) {
      if(value == null) {
         throw new IllegalStateException("Comparing key '" + name + "' with null");
      }   
      NavigableMap<T, Row> matches = tuples.tailMap(value, true);

      if (!matches.isEmpty()) {
         return new RangeKeyIndex<T>(matches);
      }
      return new SingleKeyIndex<T>();
   }

   @Override
   public ColumnIndex<T> lessThanOrEqual(T value) {
      if(value == null) {
         throw new IllegalStateException("Comparing key '" + name + "' with null");
      }   
      NavigableMap<T, Row> matches = tuples.headMap(value, true);

      if (!matches.isEmpty()) {
         return new RangeKeyIndex<T>(matches);
      }
      return new SingleKeyIndex<T>();
   }

   @Override
   public ColumnIndex<T> equalTo(T value) {
      if(value == null) {
         throw new IllegalStateException("Comparing key '" + name + "' with null");
      }   
      Row cluster = tuples.get(value);

      if (cluster == null) {
         return new SingleKeyIndex<T>();
      }
      return new SingleKeyIndex<T>(cluster);
   }

   @Override
   public ColumnIndex<T> notEqualTo(T value) {
      if(value == null) {
         throw new IllegalStateException("Comparing key '" + name + "' with null");
      }   
      Set<Row> exclude = Collections.emptySet();
      Row tuple = tuples.get(value);

      if (tuple != null) {
         exclude = Collections.singleton(tuple);
      }
      return new RangeKeyIndex<T>(tuples, exclude);
   } 
   
   @Override
   public void update(Row tuple) {
      Cell cell = tuple.getCell(index);
      T value = (T)cell.getValue();
      
      if(value == null) {
         throw new IllegalArgumentException("Key column '" + name + "' is null");
      }
      tuples.put(value, tuple);
   }   

   @Override
   public void remove(Row tuple) {
      Cell cell = tuple.getCell(index);
      T value = (T)cell.getValue();
      
      if(value == null) {
         throw new IllegalArgumentException("Key column '" + name + "' is null");
      }
      tuples.remove(value); 
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
      return tuples.size();
   }

   @Override
   public void clear() {
      tuples.clear();
   }
}
