package com.authrus.database.engine.index;

import java.util.Iterator;

import com.authrus.database.engine.Row;

public interface RowCursor extends Iterable<Row>{
   Iterator<Row> iterator();
   int count();
}
