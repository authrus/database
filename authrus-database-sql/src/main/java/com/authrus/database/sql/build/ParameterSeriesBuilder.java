package com.authrus.database.sql.build;

import static com.authrus.database.sql.parse.QueryTokenType.CLOSE;
import static com.authrus.database.sql.parse.QueryTokenType.COMMA;
import static com.authrus.database.sql.parse.QueryTokenType.EXPRESSION;
import static com.authrus.database.sql.parse.QueryTokenType.OPEN;
import static com.authrus.database.sql.parse.QueryTokenType.SELECT_VERB;
import static com.authrus.database.sql.parse.QueryTokenType.SET;
import static com.authrus.database.sql.parse.QueryTokenType.VALUES;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.authrus.database.sql.Parameter;
import com.authrus.database.sql.parse.ParameterParser;
import com.authrus.database.sql.parse.QueryToken;
import com.authrus.database.sql.parse.QueryTokenType;

public class ParameterSeriesBuilder {

   private final AtomicReference<QueryTokenType> last;
   private final List<Parameter> parameters; 
   private final ParameterParser parser;
   
   public ParameterSeriesBuilder() {
      this.last = new AtomicReference<QueryTokenType>();
      this.parameters = new ArrayList<Parameter>();
      this.parser = new ParameterParser();
   }

   public List<Parameter> createParameters() {
      return Collections.unmodifiableList(parameters);
   }

   public void update(QueryToken token) {
      QueryTokenType current = token.getType();    
      
      if(current == EXPRESSION || current == OPEN || current == SET) {
         String text = token.getToken();

         if(text != null) {
            parser.parse(text);

            if(!parser.isEmpty()) {
               List<Parameter> list = parser.getParameters();
               
               for(Parameter parameter : list) {
                  parameters.add(parameter);
               }
            }
         }
      } else if(current != CLOSE && current != VALUES && current != COMMA && current != SELECT_VERB) {      
         throw new IllegalStateException("Column series cannot accept token '" + token + "'");         
      }
      last.set(current);
   }
}
