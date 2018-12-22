package com.authrus.database.engine.statement;

import static com.authrus.database.sql.ParameterType.NAME;
import static com.authrus.database.sql.ParameterType.VALUE;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.authrus.database.Column;
import com.authrus.database.Record;
import com.authrus.database.RecordIterator;
import com.authrus.database.ResultIterator;
import com.authrus.database.Schema;
import com.authrus.database.data.DataConverter;
import com.authrus.database.data.DataType;
import com.authrus.database.engine.Catalog;
import com.authrus.database.engine.Cell;
import com.authrus.database.engine.Row;
import com.authrus.database.engine.Table;
import com.authrus.database.engine.TableModel;
import com.authrus.database.engine.filter.Filter;
import com.authrus.database.engine.filter.FilterBuilder;
import com.authrus.database.sql.Parameter;
import com.authrus.database.sql.ParameterType;
import com.authrus.database.sql.Query;

public class UpdateExecutor extends StatementExecutor {
   
   private final FilterBuilder builder;
   private final Catalog catalog;
   private final Query query;
   
   public UpdateExecutor(Catalog catalog, Query query) {
      this.builder = new FilterBuilder(catalog, query);
      this.catalog = catalog;     
      this.query = query;
   }
   
   @Override
   public ResultIterator<Record> execute() throws Exception {
      boolean disposed = closed.get();
      
      if(disposed) {
         throw new IllegalStateException("This statement has been closed");
      }
      String name = query.getTable();
      String expression = query.getSource();
      Filter updateFilter = createFilter();
      Table table = catalog.findTable(name);
      TableModel tableModel = table.getModel();
      List<Row> tuples = tableModel.list(updateFilter);
      int matchCount = tuples.size();
      
      if(matchCount > 0) {
         Cell[] updateCells = createUpdate();   
         
         for(int i = 0; i < matchCount; i++) {
            Row tuple = tuples.get(i);
            String tupleKey = tuple.getKey();
         
            if(tuple != null) {
               Cell[] mergedCells = new Cell[updateCells.length];
               
               for(int j = 0; j < mergedCells.length; j++) {
                  Cell replace = updateCells[j];
                  
                  if(replace == null) {
                     mergedCells[j] = tuple.getCell(j);
                  } else {
                     mergedCells[j] = replace;
                  } 
               }
               tuple = new Row(tupleKey, mergedCells);
            }
            tableModel.insert(tuple);
         }         
      }      
      return new RecordIterator(Collections.EMPTY_LIST, expression);
   }
   
   private Cell[] createUpdate() throws Exception {
      String name = query.getTable();
      Table table = catalog.findTable(name);
      Schema schema = table.getSchema();
      DataConverter converter = table.getConverter();
      Map<String, String> parameters = createParameters();
      int columnCount = schema.getCount();
      
      if(columnCount <= 0) {
         throw new IllegalStateException("Table '" + table + "' has no columns");
      }
      Cell[] updateCells = new Cell[columnCount];
      
      for(int i = 0; i < columnCount; i++) {
         Column column = schema.getColumn(i);
         DataType dataType = column.getDataType();
         String columnName = column.getName();
         
         if(parameters.containsKey(columnName)) {
            Comparable parameterValue = parameters.get(columnName);
            Comparable dataValue = converter.convert(dataType, parameterValue);
            
            if(dataValue != null) {
               updateCells[i] = new Cell(column, dataValue);
            } else {
               updateCells[i] = new Cell(column, null);
            }
         }
      }
      return updateCells;      
   }
   
   private Map<String, String> createParameters() throws Exception {
      String source = query.getSource();
      List<Parameter> parameters = query.getParameters();
      int count = parameters.size();
      
      if(count <= 0) {
         throw new IllegalStateException("Update '" + source + "' does not make any changes");
      }
      Map<String, String> values = new HashMap<String, String>();         
      
      for(int i = 0; i < count; i++) {
         Parameter parameter = parameters.get(i);
         String parameterName = parameter.getName();
         String parameterColumn = parameter.getColumn();
         ParameterType parameterType = parameter.getType();
         Object parameterValue = null;
         String parameterText = null;
         
         if(parameterType == VALUE) {
            parameterValue = parameter.getValue();
         } else if(parameterType == NAME) {
            parameterValue = attributes.get(parameterName);  
         } else {
            throw new IllegalStateException("Parameter '" + parameter + "' is not a named or literal parameter");            
         }       
         if(parameterValue != null) {
            parameterText = String.valueOf(parameterValue);
         }
         values.put(parameterColumn, parameterText);
      }
      return values;      
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
