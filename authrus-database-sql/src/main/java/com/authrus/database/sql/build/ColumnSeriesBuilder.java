package com.authrus.database.sql.build;

import static com.authrus.database.sql.parse.QueryTokenType.CLOSE;
import static com.authrus.database.sql.parse.QueryTokenType.COMMA;
import static com.authrus.database.sql.parse.QueryTokenType.DEFAULT;
import static com.authrus.database.sql.parse.QueryTokenType.EXPRESSION;
import static com.authrus.database.sql.parse.QueryTokenType.OPEN;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.authrus.database.Column;
import com.authrus.database.ColumnSeries;
import com.authrus.database.sql.parse.QueryToken;
import com.authrus.database.sql.parse.QueryTokenType;

public class ColumnSeriesBuilder {

   private final AtomicReference<ColumnBuilder> reference;
   private final AtomicReference<QueryTokenType> last;
   private final StringBuilder expression;
   private final AtomicInteger counter;
   private final ColumnSeries columns;
   private final AtomicInteger braces;

   public ColumnSeriesBuilder(StringBuilder expression, ColumnSeries columns) {
      this.reference = new AtomicReference<ColumnBuilder>();
      this.last = new AtomicReference<QueryTokenType>();
      this.counter = new AtomicInteger();
      this.braces = new AtomicInteger();
      this.expression = expression;
      this.columns = columns;
   }

   public ColumnSeries createSeries() {
      ColumnBuilder builder = reference.getAndSet(null);      
      
      if(builder != null) {
         Column column = builder.createColumn();         
         
         if(column != null) {
            columns.addColumn(column);
         }
      } 
      return columns;
   }

   public void update(QueryToken token) {
      QueryTokenType current = token.getType();
      QueryTokenType previous = last.get();
      
      if(current == OPEN) {
         int count = braces.incrementAndGet();
         
         if(count > 1) {
            throw new IllegalStateException("Statement '" + expression + "' did not close brace");
         }
         updateColumn(token);
      } else if(current == CLOSE) {
         int count = braces.decrementAndGet();
         
         if(count < 0) {
            throw new IllegalStateException("Statement '" + expression + "' did not open brace");
         }        
         updateColumn(token);         
      } else if(current == EXPRESSION) {
         if(previous != DEFAULT) {
            beginColumn(token);
         } else {
            updateColumn(token);        
         }
      } else if(current == COMMA) {
         finishColumn(token);     
      } else {
         updateColumn(token);
      }
      last.set(current);
   }

   private void beginColumn(QueryToken token) {
      QueryTokenType previous = last.get();
      String text = token.getToken();
      int index = counter.getAndIncrement();

      if (previous != COMMA && previous != null) {
         throw new IllegalStateException("Clause '" + expression + "' cannot accept token " + token + "");
      }
      ColumnBuilder builder = new ColumnBuilder(expression, text, index);
      
      if(text == null) {
         throw new IllegalStateException("Clause '" + expression + "' cannot process null column");
      }
      builder = reference.getAndSet(builder);
      
      if(builder != null) {
         throw new IllegalStateException("Clause '" + expression + "' did not finish column '" + builder + "'");
      }
   }
   
   private void updateColumn(QueryToken token) { 
      ColumnBuilder builder = reference.get();
      
      if(builder == null) {
         throw new IllegalStateException("Clause '" + expression + "' did not declare a column");
      }
      builder.update(token);
   } 
   
   private void finishColumn(QueryToken token) {
      QueryTokenType current = token.getType();
      ColumnBuilder builder = reference.get();      
      
      if(builder == null) {
         throw new IllegalStateException("Clause '" + expression + "' contains invalid terminal token " + token + "");
      }         
      Column column = builder.createColumn();         
      
      if(current == COMMA) {
         expression.append(", ");
      } else {
         throw new IllegalStateException("Clause '" + expression + "' contains invalid terminal token " + token + "");
      }
      columns.addColumn(column);             
      reference.set(null);
   }

   @Override
   public String toString() {
      return expression.toString();
   }
}
