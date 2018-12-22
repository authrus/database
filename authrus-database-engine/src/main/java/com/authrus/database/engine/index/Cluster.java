package com.authrus.database.engine.index;

import com.authrus.database.engine.Row;

public interface Cluster extends Iterable<Row> {
   void insert(String key, Row tuple);
   void remove(String key);
   void clear();
   int size();
}
