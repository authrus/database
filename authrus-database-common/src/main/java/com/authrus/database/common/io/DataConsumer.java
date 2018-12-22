package com.authrus.database.common.io;

public interface DataConsumer {
   void consume(DataReader reader) throws Exception;
}
