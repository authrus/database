package com.authrus.database.engine.statement;

import static com.authrus.database.data.DataConstraint.REQUIRED;
import static com.authrus.database.function.DefaultFunction.SEQUENCE;
import static com.authrus.database.sql.ParameterType.NAME;
import static com.authrus.database.sql.ParameterType.VALUE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.authrus.database.Column;
import com.authrus.database.PrimaryKey;
import com.authrus.database.Record;
import com.authrus.database.RecordIterator;
import com.authrus.database.ResultIterator;
import com.authrus.database.Schema;
import com.authrus.database.data.DataConstraint;
import com.authrus.database.data.DataConverter;
import com.authrus.database.data.DataType;
import com.authrus.database.engine.Catalog;
import com.authrus.database.engine.Cell;
import com.authrus.database.engine.Row;
import com.authrus.database.engine.Table;
import com.authrus.database.engine.TableModel;
import com.authrus.database.engine.filter.Filter;
import com.authrus.database.engine.filter.FilterBuilder;
import com.authrus.database.function.DefaultFunction;
import com.authrus.database.function.DefaultValue;
import com.authrus.database.sql.Parameter;
import com.authrus.database.sql.ParameterType;
import com.authrus.database.sql.Query;

public class InsertExecutor extends StatementExecutor {
   
   private final FilterBuilder builder;
   private final Catalog catalog;
   private final Query query;
   private final boolean replace;
   
   public InsertExecutor(Catalog catalog, Query query, boolean replace) {
      this.builder = new FilterBuilder(catalog, query);
      this.replace = replace;
      this.catalog = catalog;
      this.query = query;
   }
   
   @Override
   public ResultIterator<Record> execute() throws Exception {
      boolean disposed = closed.get();
      
      if(disposed) {
         throw new IllegalStateException("This statement has been closed");
      }
      List<Parameter> parameters = query.getParameters();
      List<String> columns = query.getColumns();
      int count = parameters.size();
      
      if(count > 0) { 
         Map<String, Comparable> values = new HashMap<String, Comparable>();
         
         for(int i = 0; i < count; i++) {
            Parameter parameter = parameters.get(i);            
            ParameterType type = parameter.getType();
            String name = parameter.getName();
            String column = columns.get(i);
            Comparable value = null;
            
            if(type == VALUE) {
               value = parameter.getValue();
            } else if(type == NAME) {
               value = attributes.get(name);
            } else {
               throw new IllegalStateException("Parameter '" + parameter + "' is not a named or literal parameter");            
            }
            values.put(column, value);
         }
         return execute(values);
      }
      return execute(Collections.EMPTY_MAP);
   }  
   
   private ResultIterator<Record> execute(Map<String, Comparable> values) throws Exception {
      String source = query.getSource();
      List<String> columns = query.getColumns();
      List<Parameter> parameters = query.getParameters();
      int parameterCount = parameters.size();
      int columnCount = columns.size();
      int valueCount = values.size();
      
      if(valueCount != columnCount) {
         throw new IllegalArgumentException("Expected " + parameterCount + " parameters but got " + valueCount + " for " + source);
      } 
      if(columnCount != parameterCount) {         
         throw new IllegalArgumentException("There are " + columnCount + " columns but " + parameterCount + " parameters for " + source);
      }
      List<String> names = query.getTables();
      int tableCount = names.size();
      
      if(tableCount > 1) {
         return copy(values);
      }
      return insert(values);
   }
   
   private ResultIterator<Record> copy(Map<String, Comparable> values) throws Exception {
      String source = query.getSource();
      List<String> tables = query.getTables();
      int tableCount = tables.size();
      
      if(tableCount < 2) {
         throw new IllegalArgumentException("Expression '" + source + "' does not specify a table to copy from");
      }
      String copyTo = tables.get(0);
      String copyFrom = tables.get(1);
      Table insertTable = catalog.findTable(copyTo);
      Table searchTable = catalog.findTable(copyFrom);
      
      if(insertTable == null) {
         throw new IllegalArgumentException("Insert '" + source + "' references unknown table '" + copyTo + "'");
      }
      if(searchTable == null) {
         throw new IllegalArgumentException("Insert '" + source + "' references unknown table '" + copyFrom + "'");
      }
      Filter selectFilter = createFilter();            
      Schema searchSchema = searchTable.getSchema();
      TableModel insertModel = insertTable.getModel();
      TableModel searchModel = searchTable.getModel();      
      List<Row> matchedTuples = searchModel.list(selectFilter);  
      
      if(!matchedTuples.isEmpty()) {
         Map<String, Comparable> newValues = new HashMap<String, Comparable>();
         List<Row> newTuples = new ArrayList<Row>();
         Set<String> newKeys = new HashSet<String>();
         
         for(Row matchedTuple : matchedTuples) {
            Set<String> insertNames = values.keySet();
            
            for(String insertName : insertNames) {
               String selectName = (String)values.get(insertName);
               Column column = searchSchema.getColumn(selectName);
               int index = column.getIndex();
               Cell cell = matchedTuple.getCell(index);
               Comparable cellValue = cell.getValue();
               
               newValues.put(insertName, cellValue);
            }
            String newKey = createKey(newValues);
            Row newTuple = createRow(newValues, newKey);
            
            if(!newKeys.add(newKey)) {
               throw new IllegalStateException("Insert '" + source + "' results in a duplicate row '" + newKey + "'");
            }
            newTuples.add(newTuple);
            newValues.clear();
         }
         for(Row newTuple : newTuples) {
            insertModel.insert(newTuple);
         }         
      }      
      return new RecordIterator(Collections.EMPTY_LIST, source);
   }
   
   private ResultIterator<Record> insert(Map<String, Comparable> values) throws Exception {
      String name = query.getTable();
      String source = query.getSource();
      String key = createKey(values);   
      Table table = catalog.findTable(name);
      TableModel tableModel = table.getModel();
      Row existing = tableModel.get(key);
   
      if(existing == null || replace) {
         Row tuple = createRow(values, key);         
      
         if(tuple != null) {
            tableModel.insert(tuple);
         }
      }      
      return new RecordIterator(Collections.EMPTY_LIST, source);
   }
   
   private String createKey(Map<String, Comparable> insertValues) throws Exception {
      String name = query.getTable();
      String expression = query.getSource();
      Table table = catalog.findTable(name);
      Schema schema = table.getSchema();
      PrimaryKey primaryKey = schema.getKey();
      List<String> keyColumns = primaryKey.getColumns();
      DataConverter dataConverter = table.getConverter();
      int keyCount = keyColumns.size();
      
      if(keyCount <= 0) {
         throw new IllegalStateException("Table " + name + " does not declare any key columns");
      }
      StringBuilder builder = new StringBuilder();
      
      for(int i = 0; i < keyCount; i++) {
         Column column = primaryKey.getColumn(i);
         String columnName = column.getName();
         DefaultValue defaultValue = column.getDefaultValue(); 
         DefaultFunction defaultFunction = defaultValue.getFunction();
         Comparable insertValue = insertValues.get(columnName);
         
         if(insertValue == null) {
            DataType dataType = column.getDataType();            
            Comparable dataValue = dataConverter.convert(dataType, null);
            Comparable resultValue = defaultValue.getDefault(column, dataValue);               
            
            if(resultValue == null) {
               throw new IllegalStateException("Key column '" + columnName + "' was null for '" + expression + "'");
            }
            insertValues.put(columnName, resultValue);               
            builder.append(resultValue);
         } else if(defaultFunction == SEQUENCE) {
            throw new IllegalStateException("Key sequence '" + columnName + "' was not null for '" + expression + "'");
         } else {               
            builder.append(insertValue);
         }
      }
      return builder.toString();
   }
   
   private Row createRow(Map<String, Comparable> insertValues, String key) throws Exception {
      String name = query.getTable();
      String expression = query.getSource();
      Table table = catalog.findTable(name);
      Schema schema = table.getSchema();
      DataConverter dataConverter = table.getConverter();
      int columnCount = schema.getCount();

      if(columnCount == 0) {
         throw new IllegalStateException("Table '" + name + "' schema contains no columns");
      }
      Cell[] insertCells = new Cell[columnCount];       
      
      for(int i = 0; i < columnCount; i++) {    
         Column column = schema.getColumn(i);
         String columnName = column.getName();
         DataType dataType = column.getDataType();    
         DataConstraint dataConstraint = column.getDataConstraint();
         DefaultValue defaultValue = column.getDefaultValue(); 
         Comparable insertValue = insertValues.get(columnName);
         
         if(insertValue == null) {          
            Comparable resultValue = defaultValue.getDefault(column, null);
            Comparable dataValue = dataConverter.convert(dataType, resultValue);
            
            if(dataValue == null && dataConstraint == REQUIRED) {
               throw new IllegalStateException("Row column '" + columnName + "' was null for '" + expression + "'");
            }
            insertValue = dataValue;              
         } else {
            insertValue = dataConverter.convert(dataType, insertValue); 
         }
         int columnIndex = column.getIndex();         

         if(insertValue != null) {
            insertCells[columnIndex] = new Cell(column, insertValue);
         } else {
            insertCells[columnIndex] = new Cell(column, null);
         }
      }
      return new Row(key, insertCells);
   }
   
   private Filter createFilter() throws Exception {
      Map<String, String> values = new HashMap<String, String>();
      
      if(!attributes.isEmpty()) {
         Set<String> names = attributes.keySet();
         
         for(String name : names) {
            Object value = attributes.get(name);
            String text = null;
            
            if(value != null) {
               text = String.valueOf(value);
            }
            values.put(name, text);
         }
      }
      return builder.createFilter(values);       
   }   
}
