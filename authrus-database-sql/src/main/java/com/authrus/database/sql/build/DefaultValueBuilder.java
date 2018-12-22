package com.authrus.database.sql.build;

import static com.authrus.database.sql.parse.QueryTokenType.CLOSE;
import static com.authrus.database.sql.parse.QueryTokenType.EXPRESSION;
import static com.authrus.database.sql.parse.QueryTokenType.OPEN;

import java.util.concurrent.atomic.AtomicReference;

import com.authrus.database.function.DefaultFunction;
import com.authrus.database.function.DefaultValue;
import com.authrus.database.sql.parse.QueryToken;
import com.authrus.database.sql.parse.QueryTokenType;

public class DefaultValueBuilder {

   private AtomicReference<QueryTokenType> last;
   private StringBuilder expression;   
   private StringBuilder argument;  
   private StringBuilder function;
   
   public DefaultValueBuilder(StringBuilder expression) {
      this.last = new AtomicReference<QueryTokenType>();
      this.argument = new StringBuilder();
      this.function = new StringBuilder();
      this.expression = expression;  
   }
   
   public DefaultValue createDefault() {
      String text = function.toString();
      int length = text.length();      
      
      if(length > 0) {
         return DefaultFunction.resolveValue(text);
      }
      return null;
   }
   
   public void update(QueryToken token) {
      QueryTokenType current = token.getType();
      QueryTokenType previous = last.get();      
      String text = token.getToken();
      
      if (current == EXPRESSION) {
         if(previous == null) {
            expression.append(text);
            function.append(text);
         } else if(previous == OPEN) {
            argument.append(text);
            expression.append(text);
            function.append(text);
         } else {
            throw new IllegalStateException("Declaration '" + expression + "' has an out of sequence token '" + token + "'");
         }        
      } else {
         if(current == CLOSE) {
            if (previous != EXPRESSION && previous != OPEN) {
               throw new IllegalStateException("Declaration '" + expression + "' has an out of sequence token '" + token + "'");
            }
            expression.append(")");
            function.append(")");
         } else if(current == OPEN) {
            
            if (previous != EXPRESSION) {
               throw new IllegalStateException("Declaration '" + expression + "' has an out of sequence token '" + token + "'");
            }
            expression.append("(");
            function.append("(");
         } else {
            throw new IllegalStateException("Declaration '" + expression + "' has an out of sequence token '" + token + "'");
         }
      }
      last.set(current);
   }
   
   @Override
   public String toString() {
      return function.toString();
   }
}
