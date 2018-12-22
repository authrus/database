package com.authrus.database.engine.index;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import com.authrus.database.engine.Row;

public class SingleKeyIndex<T extends Comparable<T>> implements ColumnIndex<T> {
   
   private final Set<Row> blank;
   private final Row tuple;

   public SingleKeyIndex() {
      this(null);
   }
   
   public SingleKeyIndex(Row tuple) {
      this.blank = Collections.emptySet();
      this.tuple = tuple;
   }

   @Override
   public ColumnIndex<T> greaterThan(T value) {
      return new SingleKeyIndex<T>();
   }

   @Override
   public ColumnIndex<T> lessThan(T value) {
      return new SingleKeyIndex<T>();
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
      return new SingleKeyIndex<T>();
   }     
   
   @Override
   public Iterator<Row> iterator() {
      Collection<Row> source = blank;
      
      if(tuple != null) {
         source = Arrays.asList(tuple);
      }
      return source.iterator();
   }   
   
   @Override
   public int count() {
      return tuple == null ? 0 :1;
   }   
}
