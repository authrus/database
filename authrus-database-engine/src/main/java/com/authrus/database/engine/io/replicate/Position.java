package com.authrus.database.engine.io.replicate;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

public class Position {

   private final Map<String, Long> counts; // transaction count
   private final Map<String, Long> times; // time of the file
   private final Set<String> tables;
   
   public Position() {
      this.counts = new ConcurrentHashMap<String, Long>();
      this.times = new ConcurrentHashMap<String, Long>();
      this.tables = new CopyOnWriteArraySet<String>();
   }
   
   public Set<String> getTables() {
      return Collections.unmodifiableSet(tables);
   }
   
   public long getCount(String table){
      Long count = counts.get(table);
      
      if(count == null) {
         return 0;
      }
      return count;
   }
   
   public void setCount(String table, long count) {
      counts.put(table, count);
   }
   
   public long getTime(String table) {
      Long time = times.get(table);
      
      if(time == null) {
         return 0;
      }
      return time;
   }
   
   public void setTime(String table, long time) {
      Long previous = times.put(table, time);
      
      if(previous == null) {
         tables.add(table);
      }
   }   
}
