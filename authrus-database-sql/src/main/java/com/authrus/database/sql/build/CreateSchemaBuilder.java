package com.authrus.database.sql.build;

import static com.authrus.database.data.DataConstraint.KEY;
import static com.authrus.database.sql.parse.QueryTokenType.CLOSE;
import static com.authrus.database.sql.parse.QueryTokenType.OPEN;
import static com.authrus.database.sql.parse.QueryTokenType.PRIMARY_KEY;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.authrus.database.Column;
import com.authrus.database.ColumnSeries;
import com.authrus.database.PrimaryKey;
import com.authrus.database.Schema;
import com.authrus.database.data.DataConstraint;
import com.authrus.database.sql.parse.QueryToken;
import com.authrus.database.sql.parse.QueryTokenType;

public class CreateSchemaBuilder {

   private final AtomicReference<QueryTokenType> last;
   private final List<QueryTokenType> history;
   private final ColumnSeriesBuilder seriesBuilder;
   private final PrimaryKeyBuilder keyBuilder;
   private final StringBuilder expression;
   private final AtomicInteger braces;
   private final Properties properties;
   private final ColumnSeries columns;
   private final ColumnSeries keys;

   public CreateSchemaBuilder() {
      this.last = new AtomicReference<QueryTokenType>();
      this.history = new LinkedList<QueryTokenType>();
      this.expression = new StringBuilder();
      this.columns = new ColumnSeries();
      this.keys = new ColumnSeries();
      this.seriesBuilder = new ColumnSeriesBuilder(expression, columns);
      this.keyBuilder = new PrimaryKeyBuilder(expression, columns, keys);
      this.braces = new AtomicInteger();
      this.properties = new Properties();
   }

   public Schema schema() {
      PrimaryKey key = keyBuilder.createKey();
      int count = key.getCount();
      
      if(count == 0) {
         List<String> names = columns.getColumns();
         
         for(String name : names) {
            Column column = columns.getColumn(name);
            DataConstraint type = column.getDataConstraint();
            
            if(type == KEY) {
               keys.addColumn(column);
               count++;
            }
         }
         if(count == 0) {
            throw new IllegalStateException("No key columns have been defined");
         }
      }
      return new Schema(key, columns, properties);
   }

   public void update(QueryToken token) {
      QueryTokenType current = token.getType();
      
      if(current == OPEN) {
         int count = braces.incrementAndGet();
         
         if(count == 1) {
            beginSchema(token);            
         } else {
            updateSchema(token);
         }
      } else if(current == CLOSE) {
         int count = braces.decrementAndGet();
         
         if(count == 0) {
            finishSchema(token);
         } else {
            updateSchema(token);
         }
      } else {
         updateSchema(token);
      }
      history.add(current);
      last.set(current);
   }
   
   private void beginSchema(QueryToken token) {
      QueryTokenType previous = last.get();

      if (previous != null) {
         throw new IllegalStateException("Clause '" + expression + "' cannot accept token " + token + "");
      }
      expression.append("(");
   }
   
   private void updateSchema(QueryToken token) {
      QueryTokenType current = token.getType();
      
      if(history.contains(PRIMARY_KEY)) {
         keyBuilder.update(token);
      } else {
         if(current != PRIMARY_KEY) {
            seriesBuilder.update(token);
         }
      }      
      history.add(current);
      last.set(current);      
   }   
   
   private void finishSchema(QueryToken token) {
      QueryTokenType previous = last.get();

      if (previous != CLOSE) {
         throw new IllegalStateException("Clause '" + expression + "' cannot accept token " + token + "");
      }
      expression.append(")");
   }   

   @Override
   public String toString() {
      return expression.toString();
   }
}
