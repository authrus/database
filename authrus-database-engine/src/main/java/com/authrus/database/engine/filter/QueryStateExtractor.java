package com.authrus.database.engine.filter;

import static com.authrus.database.sql.ParameterType.NAME;
import static com.authrus.database.sql.ParameterType.VALUE;

import java.util.List;
import java.util.Map;

import com.authrus.database.Column;
import com.authrus.database.Schema;
import com.authrus.database.data.DataConverter;
import com.authrus.database.data.DataType;
import com.authrus.database.engine.Catalog;
import com.authrus.database.engine.Table;
import com.authrus.database.sql.Condition;
import com.authrus.database.sql.Parameter;
import com.authrus.database.sql.ParameterType;
import com.authrus.database.sql.Query;
import com.authrus.database.sql.WhereClause;

public class QueryStateExtractor {

   private final Catalog catalog;
   private final Query query;

   public QueryStateExtractor(Catalog catalog, Query query) {
      this.catalog = catalog;
      this.query = query;
   }
   
   public QueryState extract(Map<String, String> parameters) {
      WhereClause clause = query.getWhereClause();
      List<Condition> conditions = clause.getConditions();
      List<String> names = query.getTables();
      int conditionCount = conditions.size();
      int nameCount = names.size();
      
      if(conditionCount > 0) {
         Comparable[] queryValues = new Comparable[conditionCount];
         String[] parameterValues = new String[conditionCount];
         
         for(int i = 0; i < conditionCount; i++) {
            Condition condition = conditions.get(i);
            Parameter parameter = condition.getParameter();
            String parameterName = parameter.getName();
            ParameterType parameterType = parameter.getType();
            
            if(parameterType == VALUE) {
               parameterValues[i] = parameter.getValue();
            } else if(parameterType == NAME) {
               parameterValues[i] = parameters.get(parameterName);
            } else {
               throw new IllegalStateException("Parameter '" + parameter + "' is not a named or literal parameter");            
            }
         }     
         String name = names.get(nameCount - 1); // get last table         
         Table table = catalog.findTable(name);
         Schema schema = table.getSchema();
         DataConverter converter = table.getConverter();
         
         for(int i = 0; i < conditionCount; i++) {
            Condition condition = conditions.get(i);
            Parameter parameter = condition.getParameter();
            String parameterColumn = parameter.getColumn();            
            Column column = schema.getColumn(parameterColumn);
            DataType dataType = column.getDataType();
            
            if(parameterValues[i] != null) {
               queryValues[i] = converter.convert(dataType, parameterValues[i]);
            }
         }
         return new QueryState(queryValues);
      }
      return new QueryState();
   }
}
