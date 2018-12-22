package com.authrus.database.engine.index;

import com.authrus.database.Column;

public interface RowSeries {
   RowSeries greaterThan(Column column, Comparable value);
   RowSeries lessThan(Column column, Comparable value);   
   RowSeries greaterThanOrEqual(Column column, Comparable value);
   RowSeries lessThanOrEqual(Column column, Comparable value);      
   RowSeries equalTo(Column column, Comparable value);  
   RowSeries notEqualTo(Column column, Comparable value);
   RowSeries like(Column column, Comparable value);   
   RowCursor createCursor();
}
