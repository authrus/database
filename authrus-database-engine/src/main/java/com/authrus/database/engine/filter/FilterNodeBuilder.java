package com.authrus.database.engine.filter;

import com.authrus.database.Column;
import com.authrus.database.Schema;
import com.authrus.database.engine.Catalog;
import com.authrus.database.engine.Table;
import com.authrus.database.sql.Query;

public class FilterNodeBuilder {
   
   private final Catalog catalog;
   private final Query query;
   
   public FilterNodeBuilder(Catalog catalog, Query query) {
      this.catalog = catalog;
      this.query = query;
   }
   
   public FilterNode compare(String name, Comparable value, String token) {
      String reference = query.getTable();
      Table table = catalog.findTable(reference);
      Schema schema = table.getSchema();
      Column column = schema.getColumn(name);
      
      if(token.equals("==")) {         
         return new ComparisonNode(column, value, token);
      }
      if(token.equals("=")) {         
         return new ComparisonNode(column, value, token);
      }      
      if(token.equals("!=")) {
         return new ComparisonNode(column, value, token);
      } 
      if(token.equals(">")) {
         return new ComparisonNode(column, value, token);
      } 
      if(token.equals("<")) {
         return new ComparisonNode(column, value, token);
      }
      if(token.equals(">=")) {
         return new ComparisonNode(column, value, token);
      } 
      if(token.equals("<=")) {
         return new ComparisonNode(column, value, token);
      }
      if(token.equals("=~")) {
         return new ComparisonNode(column, value, token);
      }
      if(token.equals("like")) {
         return new ComparisonNode(column, value, token);
      }         
      throw new IllegalArgumentException("Unknown comparison operator '" + token + "'");
   }   

   public FilterNode combine(FilterNode left, FilterNode right, String token) {
      if(token.equalsIgnoreCase("and")) {
         return new AndNode(left, right);
      }
      if(token.equalsIgnoreCase("or")) {
         return new OrNode(left, right);
      }
      throw new IllegalArgumentException("Unknown combination operator '" + token + "'");
   }   
}
