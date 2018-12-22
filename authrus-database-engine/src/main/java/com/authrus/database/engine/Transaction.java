package com.authrus.database.engine;

public interface Transaction {
   String getName();
   String getOrigin();
   String getTable();
   String getToken();
   TransactionType getType();
   Long getSequence();
   Long getTime();
}
