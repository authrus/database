package com.authrus.database.engine.io.write;

import java.io.IOException;

public interface ChangeRecordListener {
   void update(ChangeRecordBatch batch) throws IOException;
}
