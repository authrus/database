package com.authrus.database.sql;

import java.util.List;

import com.authrus.database.Schema;

public interface Query {
   Verb getVerb();
   String getName();   
   String getTable();
   List<String> getTables();   
   List<String> getColumns();
   List<Parameter> getParameters();
   WhereClause getWhereClause();   
   OrderByClause getOrderByClause();
   Schema getCreateSchema();
   String getSource();
   int getLimit();
}
