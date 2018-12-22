package com.authrus.database.sql;

import static com.authrus.database.sql.build.QueryPart.COLUMNS;
import static com.authrus.database.sql.build.QueryPart.EVERYTHING;
import static com.authrus.database.sql.build.QueryPart.KEY;
import static com.authrus.database.sql.build.QueryPart.NAME;
import static com.authrus.database.sql.build.QueryPart.ORDER_BY;
import static com.authrus.database.sql.build.QueryPart.PARAMETERS;
import static com.authrus.database.sql.build.QueryPart.ROW_LIMIT;
import static com.authrus.database.sql.build.QueryPart.TABLE;
import static com.authrus.database.sql.build.QueryPart.TYPES;
import static com.authrus.database.sql.build.QueryPart.VERB;
import static com.authrus.database.sql.build.QueryPart.WHERE_CLAUSE;

import java.util.Arrays;
import java.util.List;

import com.authrus.database.sql.build.QueryPart;

public enum Verb {
   SELECT("select", VERB, COLUMNS, TABLE, WHERE_CLAUSE, ORDER_BY, ROW_LIMIT),
   SELECT_DISTINCT("select distinct", VERB, COLUMNS, TABLE, WHERE_CLAUSE, ORDER_BY, ROW_LIMIT),
   INSERT("insert", VERB, TABLE, COLUMNS, PARAMETERS, TABLE, WHERE_CLAUSE),
   INSERT_OR_IGNORE("insert or ignore", VERB, TABLE, COLUMNS, PARAMETERS),
   UPDATE("update", VERB, TABLE, PARAMETERS, WHERE_CLAUSE),
   DELETE("delete", VERB, TABLE, WHERE_CLAUSE, ROW_LIMIT),
   CREATE_TABLE("create table if not exists", VERB, TABLE, TYPES, KEY),
   CREATE_INDEX("create index if not exists", VERB, NAME, TABLE, COLUMNS),
   DROP_TABLE("drop table if exists", VERB, TABLE),
   DROP_INDEX("drop index if exists", VERB, NAME),
   BEGIN("begin", VERB, NAME, TABLE),
   COMMIT("commit", VERB, TABLE),
   ROLLBACK("rollback", VERB, TABLE),
   ALTER("alter", EVERYTHING),
   GRANT("grant", EVERYTHING);
   
   private final String verb;
   private final QueryPart[] parts;
   
   private Verb(String verb, QueryPart... parts) {
      this.parts = parts;
      this.verb = verb;     
   }
   
   public List<QueryPart> getParts() {
      return Arrays.asList(parts);
   }
   
   public String getVerb() {
      return verb;
   }
}
