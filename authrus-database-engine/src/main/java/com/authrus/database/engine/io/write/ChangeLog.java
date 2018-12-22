package com.authrus.database.engine.io.write;


public interface ChangeLog {
   void log(ChangeRecord record);
   void start();
   void stop();
}
