package com.authrus.database.engine.io;

public interface DataBlockConsumer {
   DataBlock read(long wait);
}
