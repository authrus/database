package com.authrus.database.engine.io.read;

import java.util.Map;

public class ChangeSet {

   private final Map<Integer, Comparable> values;
   private final String key;  
   
   public ChangeSet(Map<Integer, Comparable> values, String key) {
      this.values = values;
      this.key = key;;      
   }
   
   public Map<Integer, Comparable> getChange() {
      return values;
   }
   
   public String getKey() {
      return key;
   }   
}
