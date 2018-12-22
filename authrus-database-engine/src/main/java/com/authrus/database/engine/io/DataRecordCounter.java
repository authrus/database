package com.authrus.database.engine.io;

import java.io.IOException;

public interface DataRecordCounter {
   String getOrigin() throws IOException;
   String getTable() throws IOException;
   long getTime() throws IOException;
   long getCurrent() throws IOException;
   long getNext() throws IOException;
}
