package com.authrus.database.engine.io.replicate;

import java.io.DataInputStream;
import java.io.DataOutputStream;

public interface RemoteConnection {
   DataOutputStream getOutputStream();
   DataInputStream getInputStream();
}
