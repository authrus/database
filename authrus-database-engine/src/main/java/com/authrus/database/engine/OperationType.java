package com.authrus.database.engine;

public enum OperationType {
   BEGIN("begin", "Begin a new transaction", 'T'),
   BATCH("batch", "Begin a new batch", 'B'),
   CREATE("create", "Create a new table", 'C'),
   DROP("drop", "Drop an existing table", 'D'),
   INDEX("index", "Index a column", 'I'), 
   UPDATE("update", "Update a row", 'U'),
   INSERT("insert", "Add a new row", 'A'), 
   DELETE("delete", "Remove an existing row", 'R'),
   COMMIT("commit", "Save a transaction", 'S'),
   ROLLBACK("rollback", "Rollback a transaction", 'K'); 
   
   public final String description;
   public final String name;
   public final char code;
   
   private OperationType(String name, String description, char code) {
      this.description = description;
      this.name = name;      
      this.code = code;
   }
   
   public String getDescription() {
      return description;
   }
   
   public String getName() {
      return name;
   }
   
   public char getCode() {
      return code;
   }
   
   public static OperationType resolveType(char code) {
      OperationType[] types = values();
      
      for(OperationType type : types) {
         if(type.code == code) {
            return type;
         }
      }      
      return null;
   }
   
   public static OperationType resolveType(String token) {
      OperationType[] types = values();
      
      for(OperationType type : types) {
         if(token.startsWith(type.name)) {
            return type;
         }
      }      
      return null;
   }
}
