package com.authrus.database.engine.io;

public interface DataBlock {
   String getName();
   byte[] getData();
   int getOffset();
   int getLength();
   long getTime();
}
