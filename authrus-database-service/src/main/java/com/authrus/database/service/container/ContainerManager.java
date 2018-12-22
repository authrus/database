package com.authrus.database.service.container;

import java.io.Closeable;

public interface ContainerManager extends Closeable{
   int getPort();
   boolean isDebug();
   void setDebug(boolean enable);
}
