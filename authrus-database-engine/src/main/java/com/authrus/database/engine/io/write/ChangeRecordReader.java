package com.authrus.database.engine.io.write;

import java.io.IOException;

import com.authrus.database.engine.io.DataRecordReader;
import com.authrus.database.engine.io.read.ChangeOperation;

public interface ChangeRecordReader {
   ChangeOperation read(DataRecordReader reader) throws IOException;
}
