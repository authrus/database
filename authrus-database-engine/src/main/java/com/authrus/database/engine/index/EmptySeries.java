package com.authrus.database.engine.index;

import java.util.Collection;
import java.util.Collections;

import com.authrus.database.Column;
import com.authrus.database.engine.Row;

public class EmptySeries implements RowSeries {
   
   private final Collection<Row> empty;
   
   public EmptySeries() {
      this.empty = Collections.emptyList();
   }

   @Override
   public RowCursor createCursor() {
      return new CollectionCursor(empty);
   }

   @Override
   public RowSeries greaterThan(Column column, Comparable value) {
      return new EmptySeries();
   }

   @Override
   public RowSeries lessThan(Column column, Comparable value) {
      return new EmptySeries();
   }

   @Override
   public RowSeries greaterThanOrEqual(Column column, Comparable value) {
      return new EmptySeries();
   }

   @Override
   public RowSeries lessThanOrEqual(Column column, Comparable value) {
      return new EmptySeries();
   }

   @Override
   public RowSeries equalTo(Column column, Comparable value) {
      return new EmptySeries();
   }

   @Override
   public RowSeries notEqualTo(Column column, Comparable value) {
      return new EmptySeries();
   }
   
   @Override
   public RowSeries like(Column column, Comparable value) {
      return new EmptySeries();
   } 
}
