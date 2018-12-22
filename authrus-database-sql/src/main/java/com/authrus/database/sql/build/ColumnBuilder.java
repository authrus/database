package com.authrus.database.sql.build;

import static com.authrus.database.data.DataConstraint.OPTIONAL;
import static com.authrus.database.sql.parse.QueryTokenType.BOOLEAN;
import static com.authrus.database.sql.parse.QueryTokenType.BYTE;
import static com.authrus.database.sql.parse.QueryTokenType.CHAR;
import static com.authrus.database.sql.parse.QueryTokenType.DATE;
import static com.authrus.database.sql.parse.QueryTokenType.DEFAULT;
import static com.authrus.database.sql.parse.QueryTokenType.DOUBLE;
import static com.authrus.database.sql.parse.QueryTokenType.EXPRESSION;
import static com.authrus.database.sql.parse.QueryTokenType.FLOAT;
import static com.authrus.database.sql.parse.QueryTokenType.INT;
import static com.authrus.database.sql.parse.QueryTokenType.LONG;
import static com.authrus.database.sql.parse.QueryTokenType.NOT_NULL;
import static com.authrus.database.sql.parse.QueryTokenType.PRIMARY_KEY;
import static com.authrus.database.sql.parse.QueryTokenType.SHORT;
import static com.authrus.database.sql.parse.QueryTokenType.SYMBOL;
import static com.authrus.database.sql.parse.QueryTokenType.TEXT;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.authrus.database.Column;
import com.authrus.database.data.DataConstraint;
import com.authrus.database.data.DataType;
import com.authrus.database.function.DefaultValue;
import com.authrus.database.sql.parse.QueryToken;
import com.authrus.database.sql.parse.QueryTokenType;

public class ColumnBuilder {

   private AtomicReference<QueryTokenType> last;
   private List<QueryTokenType> history;
   private DefaultValueBuilder builder;
   private StringBuilder expression;  
   private DataConstraint constraint;   
   private DataType data;
   private String name;
   private int index;
   
   public ColumnBuilder(StringBuilder expression, String name, int index) {
      this.builder = new DefaultValueBuilder(expression);  
      this.last = new AtomicReference<QueryTokenType>();      
      this.history = new ArrayList<QueryTokenType>();
      this.expression = expression;   
      this.index = index;
      this.name = name;
   }
   
   public Column createColumn() {     
      String value = createDefault();
      
      if(data == null) {
         throw new IllegalStateException("Column '" + name + "' does not have a type");
      }
      if(constraint == null) {
         return new Column(OPTIONAL, data, value, name, name, index);
      }
      return new Column(constraint, data, value, name, name, index);
   }
   
   private String createDefault() {
      DefaultValue value = builder.createDefault();
      
      if(value != null) {
         return value.getExpression();
      }      
      return null;
   }
   
   public void update(QueryToken token) {
      QueryTokenType current = token.getType();     
      
      if(history.contains(DEFAULT)) {
         builder.update(token);         
      } else {     
         if (current == INT) {
            declareType(token);
         } else if (current == DOUBLE) {
            declareType(token);
         } else if (current == FLOAT) {
            declareType(token);
         } else if (current == LONG) {
            declareType(token);
         } else if (current == SHORT) {
            declareType(token);
         } else if (current == BYTE) {
            declareType(token);
         } else if (current == BOOLEAN) {
            declareType(token);
         } else if (current == CHAR) {
            declareType(token);
         } else if (current == DATE) {
            declareType(token);           
         } else if (current == TEXT) {
            declareType(token);
         } else if (current == SYMBOL) {
            declareType(token);             
         } else if (current == NOT_NULL) {
            declareConstraint(token);
         } else if (current == PRIMARY_KEY) {
            declareConstraint(token);
         } else if(current == DEFAULT) { 
            declareDefault(token);
         } else {
            throw new IllegalStateException("Declaration '" + expression + "' cannot process token '" + token + "'");
         }
      }
      history.add(current);
      last.set(current);      
   }
   
   private void declareType(QueryToken token) {
      QueryTokenType current = token.getType();
      QueryTokenType previous = last.get();
      String text = token.getToken();
   
      if (previous != null) {
         throw new IllegalStateException("Declaration '" + expression + "' has an out of sequence token '" + token + "'");
      }
      if(data != null) {
         throw new IllegalStateException("Declaration '" + expression + "' has already declared type '" + data + "'");
      }
      expression.append(name);
      
      if (current == INT) {
         data = DataType.INT;        
      } else if (current == DOUBLE) {
         data = DataType.DOUBLE;         
      } else if (current == FLOAT) {
         data = DataType.FLOAT;        
      } else if (current == LONG) {
         data = DataType.LONG;        
      } else if (current == SHORT) {
         data = DataType.SHORT;       
      } else if (current == BYTE) {
         data = DataType.BYTE;
      } else if (current == BOOLEAN) {
         data = DataType.BOOLEAN;
      } else if (current == CHAR) {         
         data = DataType.CHAR;
      } else if (current == DATE) {
         data = DataType.DATE;                   
      } else if (current == TEXT) {
         data = DataType.TEXT;
      } else if (current == SYMBOL) {
         data = DataType.SYMBOL;           
      } else {
         throw new IllegalStateException("Declaration '" + expression + "' cannot accept type token " + token);
      }
      expression.append(" ");
      expression.append(text);
   }   
   
   private void declareConstraint(QueryToken token) {
      QueryTokenType current = token.getType();
      QueryTokenType previous = last.get();
      String text = token.getToken();
      
      if (previous == EXPRESSION) {
         throw new IllegalStateException("Declaration '" + expression + "' has an out of sequence token '" + token + "'");
      }
      if(constraint != null) {
         throw new IllegalStateException("Declaration '" + expression + "' cannot have '" + constraint + "' constraint");
      }
      if (current == NOT_NULL) {
         constraint = DataConstraint.REQUIRED;
      } else if (current == PRIMARY_KEY) {
         constraint = DataConstraint.KEY;
      } else {
         throw new IllegalStateException("Declaration '" + expression + "' cannot accept type token " + token);
      }
      expression.append(" ");
      expression.append(text);
   }
   
   private void declareDefault(QueryToken token) {
      QueryTokenType previous = last.get();
      
      if (previous == DEFAULT || previous == EXPRESSION) {
         throw new IllegalStateException("Declaration '" + expression + "' has an out of sequence token '" + token + "'");
      }    
      expression.append(" default ");     
   }    
   
   @Override
   public String toString() {
      return name;
   }
}
