package com.authrus.database.engine.index;

public interface ColumnIndex<T extends Comparable<T>> extends RowCursor {   
   ColumnIndex<T> greaterThan(T value);
   ColumnIndex<T> lessThan(T value);   
   ColumnIndex<T> greaterThanOrEqual(T value);
   ColumnIndex<T> lessThanOrEqual(T value);      
   ColumnIndex<T> equalTo(T value);  
   ColumnIndex<T> notEqualTo(T value);   
   
}
