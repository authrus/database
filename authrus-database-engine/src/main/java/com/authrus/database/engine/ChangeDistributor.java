package com.authrus.database.engine;

import java.util.Collections;
import java.util.Set;

import com.authrus.database.Schema;

public class ChangeDistributor implements ChangeListener {
   
   private final Set<ChangeListener> listeners;

   public ChangeDistributor() {
      this(Collections.EMPTY_SET);
   }
   
   public ChangeDistributor(Set<ChangeListener> listeners) {
      this.listeners = listeners;
   }  

   @Override
   public void onBegin(String origin, String table, Transaction transaction) {
      for(ChangeListener listener : listeners) {
         listener.onBegin(origin, table, transaction);
      }
   }   

   @Override
   public void onCreate(String origin, String table, Schema schema) {
      for(ChangeListener listener : listeners) {
         listener.onCreate(origin, table, schema);
      }
   }

   @Override
   public void onInsert(String origin, String table, Row tuple) {
      for(ChangeListener listener : listeners) {
         listener.onInsert(origin, table, tuple);
      }
   }   

   @Override
   public void onUpdate(String origin, String table, Row current, Row previous) {
      for(ChangeListener listener : listeners) {
         listener.onUpdate(origin, table, current, previous);
      }
   }   

   @Override
   public void onDelete(String origin, String table, String key) {
      for(ChangeListener listener : listeners) {
         listener.onDelete(origin, table, key);
      }
   }   

   @Override
   public void onIndex(String origin, String table, String column) {
      for(ChangeListener listener : listeners) {
         listener.onIndex(origin, table, column);
      }
   }

   @Override
   public void onDrop(String origin, String table) {
      for(ChangeListener listener : listeners) {
         listener.onDrop(origin, table);
      } 
   }

   @Override
   public void onCommit(String origin, String table) {
      for(ChangeListener listener : listeners) {
         listener.onCommit(origin, table);
      } 
   }

   @Override
   public void onRollback(String origin, String table) {
      for(ChangeListener listener : listeners) {
         listener.onRollback(origin, table);
      } 
   }
}
