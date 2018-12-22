package com.authrus.database.function;

import com.authrus.database.Column;

public interface DefaultValue {
   Comparable getDefault(Column column, Comparable value);
   DefaultFunction getFunction();
   String getExpression();
}
