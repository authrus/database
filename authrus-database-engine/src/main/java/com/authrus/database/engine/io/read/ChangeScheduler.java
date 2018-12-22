package com.authrus.database.engine.io.read;

public interface ChangeScheduler {
   void schedule(ChangeOperation operation);
}
