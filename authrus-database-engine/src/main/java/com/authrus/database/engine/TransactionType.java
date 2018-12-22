package com.authrus.database.engine;

public enum TransactionType {
   FULL("Atomic persistent transaction", true, true),
   ATOMIC("Atomic transient transaction", true, false),   
   PERSISTENT("Persistent transaction", false, true),
   NONE("No transaction", false, false);
   
   public final String description;
   public final boolean persistent;
   public final boolean atomic;
   
   private TransactionType(String description, boolean atomic, boolean persistent) {
      this.description = description;
      this.persistent = persistent;
      this.atomic = atomic;
   }
   
   public boolean isPersistent() {
      return persistent;
   }
   
   public boolean isAtomic() {
      return atomic; 
   }      
}
