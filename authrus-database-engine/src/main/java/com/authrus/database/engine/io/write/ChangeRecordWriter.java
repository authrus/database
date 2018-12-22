package com.authrus.database.engine.io.write;

import java.io.IOException;

import com.authrus.database.engine.io.DataRecordCounter;
import com.authrus.database.engine.io.DataRecordWriter;

public interface ChangeRecordWriter {
   void write(DataRecordWriter writer, DataRecordCounter counter) throws IOException;
}
