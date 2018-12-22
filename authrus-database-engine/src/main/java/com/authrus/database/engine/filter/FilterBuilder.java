package com.authrus.database.engine.filter;

import java.util.List;
import java.util.Map;

import com.authrus.database.engine.Catalog;
import com.authrus.database.sql.Condition;
import com.authrus.database.sql.Parameter;
import com.authrus.database.sql.Query;
import com.authrus.database.sql.WhereClause;

public class FilterBuilder {

   private final QueryStateExtractor extractor;
   private final FilterNodeBuilder builder;
   private final SortOrderEvaluator checker;
   private final Query query;

   public FilterBuilder(Catalog catalog, Query query) {
      this.extractor = new QueryStateExtractor(catalog, query);
      this.builder = new FilterNodeBuilder(catalog, query);
      this.checker = new SortOrderEvaluator(catalog, query);
      this.query = query;
   }
   
   public Filter createFilter(Map<String, String> parameters) {
      SortComparator comparator = checker.evaluateOrder();
      QueryState state = extractor.extract(parameters);
      WhereClause clause = query.getWhereClause();
      List<Condition> conditions = clause.getConditions();
      List<String> operators = clause.getOperators();      
      int count = conditions.size();
      int limit = query.getLimit();
      
      if(count > 0) {
         FilterNode[] nodes = new FilterNode[count];

         for(int i = 0; i < count; i++) {
            Condition condition = conditions.get(i);
            Parameter parameter = condition.getParameter();
            String token = condition.getComparison();        
            String column = parameter.getColumn();
            Comparable value = state.getValue(i);
            FilterNode current = builder.compare(column, value, token);
            
            if(i > 0) {
               FilterNode previous = nodes[i - 1];
               String operator = operators.get(i - 1);
               
               current = builder.combine(previous, current, operator);
            } 
            nodes[i] = current;            
         }     
         FilterNode root = nodes[count - 1];
         
         if(comparator == null) {
            return new RandomOrderFilter(root, limit);
         }
         return new SortOrderFilter(root, comparator, limit);
      }
      return new NoFilter(comparator, limit);   
   }
}
