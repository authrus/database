package com.authrus.database.engine.filter;

import com.authrus.database.Column;
import com.authrus.database.Schema;
import com.authrus.database.engine.Catalog;
import com.authrus.database.engine.Table;
import com.authrus.database.sql.OrderByClause;
import com.authrus.database.sql.Query;

public class SortOrderEvaluator {

   private final Catalog catalog;
   private final Query query;

   public SortOrderEvaluator(Catalog catalog, Query query) {   
      this.catalog = catalog;
      this.query = query;
   }
   
   public SortComparator evaluateOrder() {
      OrderByClause order = query.getOrderByClause();
      String direction = order.getDirection();
      String name = order.getColumn();
      
      if(name != null) {
         String reference = query.getTable();
         Table table = catalog.findTable(reference);
         Schema schema = table.getSchema();
         Column column = schema.getColumn(name);
         
         if(direction == null) {
            return new SortComparator(column, true); // default to asc
         }
         if(direction.equalsIgnoreCase("asc")) {
            return new SortComparator(column, true);
         }
         if(direction.equalsIgnoreCase("desc")) {
            return new SortComparator(column, false);
         }
         throw new IllegalArgumentException("Unknown sort order '" + direction + "' for '" + reference + "'");
      }
      return null; 
   }
}
