package com.authrus.database.sql.build;

import static com.authrus.database.sql.parse.QueryTokenType.CLOSE;
import static com.authrus.database.sql.parse.QueryTokenType.COMMA;
import static com.authrus.database.sql.parse.QueryTokenType.COUNT;
import static com.authrus.database.sql.parse.QueryTokenType.EXPRESSION;
import static com.authrus.database.sql.parse.QueryTokenType.OPEN;
import static com.authrus.database.sql.parse.QueryTokenType.WILD;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.authrus.database.sql.parse.QueryToken;
import com.authrus.database.sql.parse.QueryTokenType;
import com.authrus.database.sql.parse.SeriesParser;

public class NameSeriesBuilder {

   private final AtomicReference<QueryTokenType> last;
   private final List<String> columns;
   private final SeriesParser parser;
   
   public NameSeriesBuilder() {
      this.last = new AtomicReference<QueryTokenType>();
      this.columns = new ArrayList<String>();
      this.parser = new SeriesParser();
   }
   
   public List<String> createNames() {
      return Collections.unmodifiableList(columns);
   }
   
   public void update(QueryToken token) {
      QueryTokenType current = token.getType();     
      String text = token.getToken();
      
      if(current == EXPRESSION) {
         String[] list = parser.parse(text);
         
         for(String name : list) {
            if(text.startsWith("?")) {
               throw new IllegalStateException("Cannot have a ? parameter in column series");
            } else if(text.startsWith(":")) {
               throw new IllegalStateException("Cannot have named parameter '" + text + "' in column series");
            } 
            columns.add(name);            
         }
      } else if(current == COUNT) {
         columns.add("count(*)");
      } else if(current != CLOSE && current != WILD && current != COMMA && current != OPEN) {
         throw new IllegalStateException("Column series cannot accept token '" + token + "'");         
      }
      last.set(current);
   }
}
