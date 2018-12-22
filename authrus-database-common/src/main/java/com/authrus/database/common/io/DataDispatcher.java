package com.authrus.database.common.io;

import java.nio.ByteBuffer;

public interface DataDispatcher {
   void dispatch(ByteBuffer buffer) throws Exception;
}
