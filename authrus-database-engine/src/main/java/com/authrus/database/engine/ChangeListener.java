package com.authrus.database.engine;

import com.authrus.database.Schema;

public interface ChangeListener {
   void onBegin(String origin, String table, Transaction transaction);
   void onCreate(String origin, String table, Schema schema);
   void onInsert(String origin, String table, Row tuple);
   void onUpdate(String origin, String table, Row current, Row previous);
   void onDelete(String origin, String table, String key);
   void onIndex(String origin, String table, String column);
   void onCommit(String origin, String table);
   void onRollback(String origin, String table);   
   void onDrop(String origin, String table);
}
