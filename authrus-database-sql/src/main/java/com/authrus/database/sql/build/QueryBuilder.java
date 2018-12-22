package com.authrus.database.sql.build;

import static com.authrus.database.sql.Verb.BEGIN;
import static com.authrus.database.sql.Verb.COMMIT;
import static com.authrus.database.sql.Verb.CREATE_INDEX;
import static com.authrus.database.sql.Verb.CREATE_TABLE;
import static com.authrus.database.sql.Verb.DELETE;
import static com.authrus.database.sql.Verb.DROP_INDEX;
import static com.authrus.database.sql.Verb.DROP_TABLE;
import static com.authrus.database.sql.Verb.INSERT;
import static com.authrus.database.sql.Verb.INSERT_OR_IGNORE;
import static com.authrus.database.sql.Verb.ROLLBACK;
import static com.authrus.database.sql.Verb.SELECT;
import static com.authrus.database.sql.Verb.SELECT_DISTINCT;
import static com.authrus.database.sql.Verb.UPDATE;
import static com.authrus.database.sql.build.QueryPart.COLUMNS;
import static com.authrus.database.sql.build.QueryPart.KEY;
import static com.authrus.database.sql.build.QueryPart.NAME;
import static com.authrus.database.sql.build.QueryPart.ORDER_BY;
import static com.authrus.database.sql.build.QueryPart.PARAMETERS;
import static com.authrus.database.sql.build.QueryPart.ROW_LIMIT;
import static com.authrus.database.sql.build.QueryPart.TABLE;
import static com.authrus.database.sql.build.QueryPart.TYPES;
import static com.authrus.database.sql.build.QueryPart.WHERE_CLAUSE;
import static com.authrus.database.sql.parse.QueryTokenType.BEGIN_VERB;
import static com.authrus.database.sql.parse.QueryTokenType.COMMIT_VERB;
import static com.authrus.database.sql.parse.QueryTokenType.CREATE_INDEX_VERB;
import static com.authrus.database.sql.parse.QueryTokenType.CREATE_TABLE_VERB;
import static com.authrus.database.sql.parse.QueryTokenType.DELETE_VERB;
import static com.authrus.database.sql.parse.QueryTokenType.DROP_INDEX_VERB;
import static com.authrus.database.sql.parse.QueryTokenType.DROP_TABLE_VERB;
import static com.authrus.database.sql.parse.QueryTokenType.EXPRESSION;
import static com.authrus.database.sql.parse.QueryTokenType.INSERT_OR_IGNORE_VERB;
import static com.authrus.database.sql.parse.QueryTokenType.INSERT_VERB;
import static com.authrus.database.sql.parse.QueryTokenType.ROLLBACK_VERB;
import static com.authrus.database.sql.parse.QueryTokenType.SELECT_DISTINCT_VERB;
import static com.authrus.database.sql.parse.QueryTokenType.SELECT_VERB;
import static com.authrus.database.sql.parse.QueryTokenType.UPDATE_VERB;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.authrus.database.Schema;
import com.authrus.database.sql.OrderByClause;
import com.authrus.database.sql.Parameter;
import com.authrus.database.sql.Query;
import com.authrus.database.sql.Verb;
import com.authrus.database.sql.WhereClause;
import com.authrus.database.sql.parse.QueryToken;
import com.authrus.database.sql.parse.QueryTokenType;

public class QueryBuilder {

   private final AtomicReference<String> name;
   private final AtomicReference<String> table;
   private final AtomicReference<Verb> verb;
   private final LinkedList<String> tables;  
   private final CreateSchemaBuilder create;
   private final ParameterSeriesBuilder parameters; 
   private final NameSeriesBuilder columns;
   private final OrderByClauseBuilder order;
   private final WhereClauseBuilder where;
   private final AtomicInteger position;
   private final LimitBuilder limit;


   public QueryBuilder() {
      this.name = new AtomicReference<String>();   
      this.table = new AtomicReference<String>();      
      this.tables = new LinkedList<String>();
      this.verb = new AtomicReference<Verb>();
      this.create = new CreateSchemaBuilder();
      this.parameters = new ParameterSeriesBuilder();
      this.columns = new NameSeriesBuilder();
      this.where = new WhereClauseBuilder();
      this.order = new OrderByClauseBuilder();
      this.position = new AtomicInteger();
      this.limit = new LimitBuilder();
   }
   
   public Query createCommand(String original) {
      return new Delegate(original);
   }
   
   public void update(QueryToken token) {
      Verb type = verb.get();
      
      if(type == null) {
         beginCommand(token);
      } else {
         List<QueryPart> parts = type.getParts();
         int current = position.get();
         int count = parts.size();
         
         while(current < count) {
            QueryPart part = parts.get(current);
            
            if(!part.accept(token)) {
               if(current < count) {
                  current = position.incrementAndGet();                  
                  part = parts.get(current);
               }
            }
            if(part.accept(token)) {
               if(part == COLUMNS) {
                  defineColumns(token);               
               } else if(part == PARAMETERS) {
                  defineParameters(token);               
               } else if(part == WHERE_CLAUSE) {
                  defineWhereClause(token);
               } else if(part == ORDER_BY) {
                  defineOrderBy(token);
               } else if(part == TYPES) {
                  defineTypes(token);               
               } else if(part == KEY) {
                  defineKey(token);
               } else if(part == NAME) {
                  declareName(token);                  
               } else if(part == TABLE) {
                  declareTable(token);
               } else if(part == ROW_LIMIT) {
                  defineRowLimit(token);                  
               } else {
                  throw new IllegalStateException("Token '" + token + "' could not be consumed");
               }
               break;
            }
         }
      }
   }
   
   private void beginCommand(QueryToken token) {
      QueryTokenType type = token.getType();
      
      if(type == SELECT_VERB) {
         verb.set(SELECT);         
      } else if(type == SELECT_DISTINCT_VERB) {
         verb.set(SELECT_DISTINCT);
      } else if(type == INSERT_VERB) {
         verb.set(INSERT);
      } else if(type == INSERT_OR_IGNORE_VERB) {
         verb.set(INSERT_OR_IGNORE);         
      } else if(type == DELETE_VERB) {
         verb.set(DELETE);
      } else if(type == UPDATE_VERB) {
         verb.set(UPDATE);
      } else if(type == CREATE_TABLE_VERB) {
         verb.set(CREATE_TABLE);
      } else if(type == CREATE_INDEX_VERB) {
         verb.set(CREATE_INDEX);
      } else if(type == DROP_TABLE_VERB) {
         verb.set(DROP_TABLE);
      } else if(type == DROP_INDEX_VERB) {
         verb.set(DROP_INDEX);                  
      } else if(type == BEGIN_VERB) {
         verb.set(BEGIN);
      } else if(type == COMMIT_VERB) {
         verb.set(COMMIT);
      } else if(type == ROLLBACK_VERB) {
         verb.set(ROLLBACK);         
      } else {
         throw new IllegalStateException("Unable to interpret verb " + token);
      }
   }
   
   private void declareTable(QueryToken token) {
      QueryTokenType type = token.getType();
      
      if(type == EXPRESSION) {
         String value = token.getToken();
         
         table.compareAndSet(null, value);
         tables.add(value);                  
      }
   }
   
   private void declareName(QueryToken token) {
      QueryTokenType type = token.getType();
      
      if(type == EXPRESSION) {
         String value = token.getToken();
         
         if(!name.compareAndSet(null, value)) {
            throw new IllegalStateException("Name '" + value + "' invalid as already declared '" + name + "'");
         }
      }
   }   
   
   private void defineColumns(QueryToken token) {
      columns.update(token);
   }
   
   private void defineParameters(QueryToken token) {
      parameters.update(token);
   }
   
   private void defineWhereClause(QueryToken token) {
      where.update(token);
   }
   
   private void defineOrderBy(QueryToken token) {
      order.update(token);
   }
   
   private void defineRowLimit(QueryToken token) {
      limit.update(token);
   }   
   
   private void defineTypes(QueryToken token) {
      create.update(token);
   }
   
   private void defineKey(QueryToken token) {
      create.update(token);
   }   
   
   private class Delegate implements Query {
      
      private final String original;
      
      public Delegate(String original) {
         this.original = original;
      }
   
      @Override
      public Verb getVerb() {
         return verb.get();
      }
      
      @Override
      public List<String> getColumns() {
         return columns.createNames();
      }
      
      @Override
      public List<Parameter> getParameters() {
         return parameters.createParameters();
      }
      
      @Override
      public String getTable() {
         return table.get();
      }
      
      @Override
      public List<String> getTables() {
         return Collections.unmodifiableList(tables);
      }      
      
      @Override
      public String getName() {
         return name.get();
      }      
      
      @Override
      public WhereClause getWhereClause() {
         return where.createClause();
      }
      
      @Override
      public OrderByClause getOrderByClause() {
         return order.createClause();
      }
      
      @Override
      public Schema getCreateSchema() {
         return create.schema();
      }   
      
      @Override
      public int getLimit() {
         return limit.createLimit();
      }
      
      @Override
      public String getSource() {
         return original;
      }
      
      @Override
      public String toString() {
         return original;
      }
   }
}
