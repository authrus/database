package com.authrus.database;

import java.util.ArrayList;
import java.util.List;

import com.authrus.database.common.collection.Cache;
import com.authrus.database.common.collection.LeastRecentlyUsedCache;

public class ResultCache {

   private final Cache<String, CacheSection> sections; 
   
   public ResultCache() {
      this(100);
   }
   
   public ResultCache(int capacity) {
      this.sections = new LeastRecentlyUsedCache<String, CacheSection>(capacity);
   }
   
   public List<Record> check(String table, String key) throws Exception {
      CacheSection section = section(table);
      
      if(section != null) {
         return section.check(key);
      }
      return null;
   }
   
   public void update(String table, String key, List<Record> result) throws Exception {
      CacheSection section = section(table);
      
      if(section != null) {
         section.update(key, result);
      }      
   }
   
   public void clear(String table) throws Exception {
      CacheSection section = section(table);
      
      if(section != null) {
         section.clear();
      }  
   }
   
   public void clear() throws Exception {
      sections.clear();
   }   
   
   private CacheSection section(String table) throws Exception {
      CacheSection section = sections.fetch(table);
      
      if(section == null) {
         CacheSection empty = new CacheSection(table);
         
         if(table != null) {
            sections.cache(table, empty);
         }
         return empty;
      }
      return section;
   }   
   
   private static class CacheSection {
    
      private final Cache<String, List<Record>> records; 
      private final String table;
      
      public CacheSection(String table) {
         this(table, 100);
      }
      
      public CacheSection(String table, int capacity) {
         this.records = new LeastRecentlyUsedCache<String, List<Record>>(capacity);
         this.table = table;
      }
      
      public List<Record> check(String key) throws Exception {
         List<Record> store = records.fetch(key);
         
         if(store != null) {
            return new ArrayList<Record>(store);
         }
         return null;
      }
      
      public void update(String key, List<Record> result) throws Exception {
         List<Record> store = new ArrayList<Record>(result);
         
         if(key != null) {
            records.cache(key, store);
         }
      }
      
      public void clear() throws Exception {
         records.clear();
      }
      
      @Override
      public String toString() {
         return table;
      }
   }
}
