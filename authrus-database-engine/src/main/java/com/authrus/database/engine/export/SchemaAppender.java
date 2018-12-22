package com.authrus.database.engine.export;

import java.io.IOException;

import com.authrus.database.Column;
import com.authrus.database.Schema;

public class SchemaAppender {

   private final Appendable appender;
   
   public SchemaAppender(Appendable appender) {
      this.appender = appender;
   }
   
   public void append(Schema schema) throws IOException {
      int count = schema.getCount();
      
      for(int i = 0; i < count; i++) {
         Column column = schema.getColumn(i);
         String title = column.getTitle();
         
         if(i > 0){
            appender.append(',');
         }
         appender.append(title);
      }
   }   
}
