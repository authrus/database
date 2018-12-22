package com.authrus.database.engine.io.read;

import java.util.HashMap;
import java.util.Map;

import com.authrus.database.Column;
import com.authrus.database.Schema;
import com.authrus.database.engine.Catalog;
import com.authrus.database.engine.Cell;
import com.authrus.database.engine.Row;
import com.authrus.database.engine.Table;

public class ChangeSetMerger {

   private final Map<String, TableState> tables;
   private final Catalog catalog;

   public ChangeSetMerger(Catalog catalog) {
      this.tables = new HashMap<String, TableState>();
      this.catalog = catalog;
   }
   
   public Row insertState(String table, ChangeSet change) {
      TableState state = tables.get(table);
      
      if(state == null) {
         TableState newState = createState(table);
         Row newRow = newState.insertRow(table, change);
         
         tables.put(table, newState);         
         return newRow;         
      }
      return state.insertRow(table, change);
   }   
   
   public Row updateState(String table, ChangeSet change) {
      TableState state = tables.get(table);
      
      if(state == null) {
         throw new IllegalStateException("Table '" + table + "' does not exist for update");
      }
      return state.mergeRow(table, change);
   }
   
   public Row deleteState(String table, String key) {
      TableState state = tables.get(table);
      
      if(state == null) {
         throw new IllegalStateException("Table '" + table + "' does not exist for delete");
      }
      return state.deleteRow(table, key);
   }
   
   public void dropState(String name) {
      tables.remove(name); 
   }
   
   private TableState createState(String name) {
      Table table = catalog.findTable(name);
      
      if(table == null) {
         throw new IllegalStateException("Table '" + name + "' does not exist");
      }
      Schema schema = table.getSchema();
      
      if(schema == null) {
         throw new IllegalStateException("No schema found for " + name);
      }
      return new TableState(schema);
   }

   private class TableState {

      private final Map<String, RowTupleState> rows;
      private final Schema schema;

      public TableState(Schema schema) {
         this.rows = new HashMap<String, RowTupleState>();
         this.schema = schema;
      }
      
      public Row deleteRow(String table, String key) {
         RowTupleState currentState = rows.remove(key);

         if (currentState == null) {
            throw new IllegalStateException("Key '" + key + "' does not exists in '" + table + "'");
         }
         return currentState.deleteRow();
      }      
      
      public Row insertRow(String table, ChangeSet change) {
         String key = change.getKey();
         RowTupleState currentState = rows.get(key);

         if (currentState != null) {
            throw new IllegalStateException("Key '" + key + "' already exists in '" + table + "'");
         }
         RowTupleState newState = createRow(table, change);
         Row newRow = newState.createRow();

         rows.put(key, newState);
         return newRow;
      }

      public Row mergeRow(String table, ChangeSet change) {
         String key = change.getKey();
         RowTupleState currentState = rows.get(key);

         if (currentState == null) {
            RowTupleState newState = createRow(table, change);
            Row newRow = newState.createRow();

            rows.put(key, newState);
            return newRow;
         }
         Cell[] rowChange = createChange(table, change);
         
         if(rowChange == null) {
            rows.remove(key);
            return null;
         }
         return currentState.updateRow(rowChange);
      }

      public RowTupleState createRow(String table, ChangeSet change) {
         Map<Integer, Comparable> attributes = change.getChange();
         String key = change.getKey();
         int columnCount = schema.getCount();
         int changeCount = attributes.size();

         if (columnCount != changeCount) {
            throw new IllegalStateException("Insert requires '" + columnCount + "' but '" + key + "' contains " + changeCount);
         }
         Cell[] cells = new Cell[changeCount];

         for (int i = 0; i < changeCount; i++) {
            Column column = schema.getColumn(i);
            Comparable value = attributes.get(i);

            cells[i] = new Cell(column, value);
         }
         return new RowTupleState(key, cells);
      }

      public Cell[] createChange(String table, ChangeSet change) {
         Map<Integer, Comparable> attributes = change.getChange();
         int changeCount = attributes.size();
         int columnCount = schema.getCount();         
         
         if(changeCount > 0) {
            Cell[] cells = new Cell[columnCount];
   
            for (int i = 0; i < columnCount; i++) {
               Column column = schema.getColumn(i);
               Comparable value = attributes.get(i);

               if(value != null) {
                  cells[i] = new Cell(column, value);
               }
            }
            return cells;
         }
         return null;
      }
   }

   private class RowTupleState {

      private final Cell[] cells;
      private final String key;

      public RowTupleState(String key, Cell[] cells) {
         this.cells = cells;
         this.key = key;
      }
      
      public Row deleteRow() {
         return new Row(key, cells);
      }      

      public Row createRow() {
         Cell[] merge = new Cell[cells.length];
         
         for(int i = 0; i < merge.length; i++) {
            merge[i] = cells[i];
         }
         return new Row(key, merge);
      }

      public Row updateRow(Cell[] change) {
         Cell[] merge = new Cell[cells.length];
         
         for (int i = 0; i < change.length; i++) {
            Cell cell = change[i];

            if (cell != null) {
               cells[i] = cell; // keep for next time
               merge[i] = cell;
            } else {
               merge[i] = cells[i];
            }
         }
         return new Row(key, merge);
      }
   }
}
