package com.authrus.database.engine.filter;

import java.util.Iterator;

import com.authrus.database.engine.Row;
import com.authrus.database.engine.index.RowSeries;

public interface Filter {
   Iterator<Row> select(RowSeries series);
   int count(RowSeries series);
}
