package com.authrus.database.engine.io.read;

import com.authrus.database.Schema;
import com.authrus.database.engine.Transaction;

public interface ChangeAssembler {
   void onBegin(String origin, String name, Transaction transaction);
   void onCreate(String origin, String name, Schema schema);
   void onInsert(String origin, String name, ChangeSet change);
   void onUpdate(String origin, String name, ChangeSet change);
   void onDelete(String origin, String name, String key);
   void onIndex(String origin, String name, String column);
   void onRollback(String origin, String name);   
   void onCommit(String origin, String name);
   void onDrop(String origin, String name);
}
