package com.authrus.database.engine.index;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import com.authrus.database.Column;
import com.authrus.database.engine.Row;
import com.authrus.database.engine.TableState;
import com.authrus.database.engine.predicate.Predicate;
import com.authrus.database.engine.predicate.PredicateBuilder;

public class ColumnIndexSeries implements RowSeries {
   
   private final Map<String, ? extends ColumnIndex> indexes;
   private final PredicateBuilder builder; 
   private final ColumnIndex previous;
   private final TableState tuples;

   public ColumnIndexSeries(Map<String, ? extends ColumnIndex> indexes, TableState tuples) {
      this(indexes, tuples, null);
   }
   
   public ColumnIndexSeries(Map<String, ? extends ColumnIndex> indexes,  TableState tuples, ColumnIndex previous) {
      this.builder = new PredicateBuilder();
      this.previous = previous;
      this.indexes = indexes;
      this.tuples = tuples;     
   }        

   @Override
   public RowCursor createCursor() {
      Collection<Row> list = tuples.values();
      
      if(previous != null) {
         return previous;
      }
      return new CollectionCursor(list);
   }

   @Override
   public RowSeries greaterThan(Column column, Comparable value) {      
      String name = column.getName();
      ColumnIndex index = indexes.get(name);
      
      if(index != null) {
         ColumnIndex result = index.greaterThan(value);
         Map<String, ColumnIndex> indexes = Collections.singletonMap(name, result);
               
         return new ColumnIndexSeries(indexes, tuples, result);
      }
      return where(column, value, ">");

   }

   @Override
   public RowSeries lessThan(Column column, Comparable value) {
      String name = column.getName();
      ColumnIndex index = indexes.get(name);
      
      if(index != null) {
         ColumnIndex result = index.lessThan(value);
         Map<String, ColumnIndex> indexes = Collections.singletonMap(name, result);
               
         return new ColumnIndexSeries(indexes, tuples, result);
      }
      return where(column, value, "<");
   }

   @Override
   public RowSeries greaterThanOrEqual(Column column, Comparable value) {
      String name = column.getName();
      ColumnIndex index = indexes.get(name);
      
      if(index != null) {
         ColumnIndex result = index.greaterThanOrEqual(value);
         Map<String, ColumnIndex> indexes = Collections.singletonMap(name, result);
               
         return new ColumnIndexSeries(indexes, tuples, result);
      }
      return where(column, value, ">=");
   }

   @Override
   public RowSeries lessThanOrEqual(Column column, Comparable value) {
      String name = column.getName();
      ColumnIndex index = indexes.get(name);
      
      if(index != null) {
         ColumnIndex result = index.lessThanOrEqual(value);
         Map<String, ColumnIndex> indexes = Collections.singletonMap(name, result);
               
         return new ColumnIndexSeries(indexes, tuples, result);
      }
      return where(column, value, "<=");
   }

   @Override
   public RowSeries equalTo(Column column, Comparable value) {
      String name = column.getName();
      ColumnIndex index = indexes.get(name);
      
      if(index != null) {
         ColumnIndex result = index.equalTo(value);
         Map<String, ColumnIndex> indexes = Collections.singletonMap(name, result);
               
         return new ColumnIndexSeries(indexes, tuples, result);
      }
      return where(column, value, "==");
   }

   @Override
   public RowSeries notEqualTo(Column column, Comparable value) {
      String name = column.getName();
      ColumnIndex index = indexes.get(name);
      
      if(index != null) {
         ColumnIndex result = index.notEqualTo(value);
         Map<String, ColumnIndex> indexes = Collections.singletonMap(name, result);
               
         return new ColumnIndexSeries(indexes, tuples, result);
      }
      return where(column, value, "!=");
   }
   
   @Override
   public RowSeries like(Column column, Comparable value) {
      return where(column, value, "=~");
   }   
   
   protected RowSeries where(Column column, Comparable value, String operator) {
      Predicate predicate = builder.compare(column, value, operator);
      RowCursor cursor = previous;
      
      if(cursor == null) {
         Collection<Row> list = tuples.values();
         
         if(!list.isEmpty()) {
            cursor = new CollectionCursor(list);
         }
      }
      if(cursor != null) {
         return new PredicateSeries(cursor, predicate);
      }
      return  new EmptySeries();
   }       
}
