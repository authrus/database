package com.authrus.database.sql.build;

import static com.authrus.database.data.DataConstraint.KEY;
import static com.authrus.database.sql.parse.QueryTokenType.CLOSE;
import static com.authrus.database.sql.parse.QueryTokenType.COMMA;
import static com.authrus.database.sql.parse.QueryTokenType.EXPRESSION;
import static com.authrus.database.sql.parse.QueryTokenType.OPEN;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.authrus.database.Column;
import com.authrus.database.ColumnSeries;
import com.authrus.database.PrimaryKey;
import com.authrus.database.data.DataConstraint;
import com.authrus.database.sql.parse.QueryToken;
import com.authrus.database.sql.parse.QueryTokenType;

public class PrimaryKeyBuilder {

   private final AtomicReference<QueryTokenType> last;  
   private final StringBuilder expression;
   private final ColumnSeries columns;
   private final ColumnSeries keys;

   public PrimaryKeyBuilder(StringBuilder expression, ColumnSeries columns, ColumnSeries keys) {
      this.last = new AtomicReference<QueryTokenType>();    
      this.expression = expression;
      this.columns = columns;
      this.keys = keys;
   }

   public PrimaryKey createKey() {
      int count = keys.getCount();
      
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
      return new PrimaryKey(keys);
   }

   public void update(QueryToken token) {
      QueryTokenType type = token.getType();

      if (type == OPEN) {
         beginPrimaryKey(token);
      } else if (type == EXPRESSION || type == COMMA) {
         updatePrimaryKey(token);
      } else if(type == CLOSE) {
         finishPrimaryKey(token);
      } else {
         throw new IllegalStateException("Statement '" + expression + "' is unable to process " + token);
      }      
      last.set(type);
   }

   private void beginPrimaryKey(QueryToken token) {
      int count = columns.getCount();
      
      if(count == 0) {
         throw new IllegalStateException("Clause '" + expression + "' did not declare any columns");
      }   
      expression.append("primary key (");
   }
   
   private void updatePrimaryKey(QueryToken token) {
      QueryTokenType type = token.getType();
      String name = token.getToken();
      
      if (type == EXPRESSION) {
         Column column = columns.getColumn(name);
         
         if(column == null) {
            throw new IllegalStateException("Clause '" + expression + "' references invalid key " + name + "");
         }
         int count = keys.getCount();
         
         if(count > 0) {
            expression.append(", ");
         }
         keys.addColumn(column);
         expression.append(name);
      }
   }   
   
   private void finishPrimaryKey(QueryToken token) {
      QueryTokenType previous = last.get();

      if (previous != EXPRESSION && previous != CLOSE) {
         throw new IllegalStateException("Clause '" + expression + "' cannot accept token " + token + "");
      }
      expression.append(")");
   }

   @Override
   public String toString() {
      return expression.toString();
   }
}
