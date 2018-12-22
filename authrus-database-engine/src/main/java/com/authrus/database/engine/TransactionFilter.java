package com.authrus.database.engine;

import com.authrus.database.engine.Transaction;

public interface TransactionFilter {
   boolean accept(Transaction transaction);
}
