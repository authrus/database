package com.authrus.database.engine.filter;

import com.authrus.database.engine.index.RowSeries;

public interface FilterNode {
   RowSeries apply(RowSeries series);
}
