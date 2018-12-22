package com.authrus.database.sql.build;

import static com.authrus.database.sql.parse.QueryTokenType.LIMIT;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.authrus.database.sql.parse.QueryToken;
import com.authrus.database.sql.parse.QueryTokenType;

public class LimitBuilder {

   private final AtomicReference<QueryTokenType> last;
   private final AtomicInteger limit;
   
   public LimitBuilder() {
      this.last = new AtomicReference<QueryTokenType>();
      this.limit = new AtomicInteger();
   }
   
   public int createLimit() {
      return limit.get();
   }
   
   public void update(QueryToken token) {
      QueryTokenType current = token.getType();     
      String text = token.getToken();
      
      if(current != LIMIT) {
         QueryTokenType previous = last.get();
         
         if(previous != LIMIT) {
            throw new IllegalStateException("Limit clause must be a single digit");
         }
         int value = Integer.parseInt(text);
         
         if(value <= 0) {
            throw new IllegalStateException("Limit '" + text + "' is not greater than zero");
         }
         limit.set(value);
      }
      last.set(current);
   }
}
