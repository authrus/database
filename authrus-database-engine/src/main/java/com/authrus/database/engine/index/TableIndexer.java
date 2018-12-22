package com.authrus.database.engine.index;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.authrus.database.Column;
import com.authrus.database.ColumnSeries;
import com.authrus.database.PrimaryKey;
import com.authrus.database.engine.Row;
import com.authrus.database.engine.TableState;
import com.authrus.database.engine.filter.Filter;

public class TableIndexer {
   
   private final Map<String, ColumnIndexUpdater> updaters;
   private final ColumnIndexBuilder builder;
   private final ColumnSeries indexes;
   private final RowSeries series;
   private final TableState state;
   
   public TableIndexer(PrimaryKey key, TableState state) {
      this.updaters = new HashMap<String, ColumnIndexUpdater>();
      this.series = new ColumnIndexSeries(updaters, state);
      this.builder = new ColumnIndexBuilder(key);
      this.indexes = new ColumnSeries();
      this.state = state;
   }
   
   public void index(Column column) {
      String name = column.getName();
      
      if(!updaters.containsKey(name)) {               
         ColumnIndexUpdater updater = builder.create(column);
      
         if(!state.isEmpty()) {
            Set<String> keys = state.keys();
            
            for(String key : keys) {
               Row tuple = state.get(key);
               
               if(tuple != null) {
                  updater.update(tuple);
               }
            }
         }
         indexes.addColumn(column);
         updaters.put(name, updater);
      }
   }
   
   public int count(Filter filter) {
      return filter.count(series);
   }   
   
   public Iterator<Row> select(Filter filter) {
      return filter.select(series);
   }
   
   public Row insert(String key, Row tuple) {
      int count = indexes.getCount();
      
      for(int i = 0; i < count; i++) {
         Column column = indexes.getColumn(i);
         String name = column.getName();
         ColumnIndexUpdater updater = updaters.get(name);
         
         updater.update(tuple);
      }
      return state.insert(key, tuple);     
   }
   
   public Row remove(String key) {
      Row tuple = state.remove(key);
      
      if(tuple != null) {
         int count = indexes.getCount();
         
         for(int i = 0; i < count; i++) {
            Column column = indexes.getColumn(i);
            String name = column.getName();
            ColumnIndexUpdater updater = updaters.get(name);
            
            updater.remove(tuple);
         }
      }
      return tuple;
   }
   
   public boolean clear() {
      int count = indexes.getCount();
      
      for(int i = 0; i < count; i++) {
         Column column = indexes.getColumn(i);
         String name = column.getName();
         ColumnIndexUpdater updater = updaters.get(name);
         
         updater.clear();
      }
      return count > 0; // did anything change
   }

}
