package com.authrus.database.engine.index;

import com.authrus.database.engine.Row;

public interface ColumnIndexUpdater<T extends Comparable<T>>  extends ColumnIndex<T> {
   void update(Row tuple);
   void remove(Row tuple);
   void clear();
}
