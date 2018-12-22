package com.authrus.database.engine.index;

import com.authrus.database.Column;
import com.authrus.database.engine.predicate.Predicate;
import com.authrus.database.engine.predicate.PredicateBuilder;

public class PredicateSeries implements RowSeries {
   
   private final PredicateBuilder builder;
   private final RowCursor cursor;
   private final Predicate predicate; 
   
   public PredicateSeries(RowCursor cursor, Predicate predicate) {
      this.builder = new PredicateBuilder();
      this.predicate = predicate;
      this.cursor = cursor;
   }
   
   @Override
   public RowCursor createCursor() {
      return new PredicateCursor(cursor, predicate);
   }   

   @Override
   public RowSeries greaterThan(Column column, Comparable value) {
      return where(column, value, ">");
   }

   @Override
   public RowSeries lessThan(Column column, Comparable value) {
      return where(column, value, "<");
   }

   @Override
   public RowSeries greaterThanOrEqual(Column column, Comparable value) {
      return where(column, value, ">=");
   }

   @Override
   public RowSeries lessThanOrEqual(Column column, Comparable value) {
      return where(column, value, "<=");
   }

   @Override
   public RowSeries equalTo(Column column, Comparable value) {
      return where(column, value, "==");
   }

   @Override
   public RowSeries notEqualTo(Column column, Comparable value) {
      return where(column, value, "!=");
   }
   
   @Override
   public RowSeries like(Column column, Comparable value) {
      return where(column, value, "=~");
   }   
   
   protected RowSeries where(Column column, Comparable value, String operator) {
      Predicate comparison = builder.compare(column, value, operator);
      Predicate combination = builder.combine(predicate, comparison, "and");
      
      return new PredicateSeries(cursor, combination);
   }    
}
