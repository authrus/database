package com.authrus.database.sql;

import java.util.List;

public class WhereClause {
   
   private final List<Condition> conditions;
   private final List<String> operators;
   private final String clause;

   public WhereClause(String clause, List<Condition> parameters, List<String> operators) {
      this.conditions = parameters;
      this.operators = operators;
      this.clause = clause;
   }
   
   public List<Condition> getConditions() {
      return conditions;
   }   
   
   public List<String> getOperators() {
      return operators;
   }
   
   public String getClause() {
      return clause;
   }
   
   @Override
   public String toString() {
      return clause;
   }
}
