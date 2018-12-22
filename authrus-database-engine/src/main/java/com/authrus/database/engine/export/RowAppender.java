package com.authrus.database.engine.export;

import java.io.IOException;

import com.authrus.database.Column;
import com.authrus.database.data.DataType;
import com.authrus.database.engine.Cell;
import com.authrus.database.engine.Row;

public class RowAppender {

   private final ValueEscaper escaper;
   private final Appendable appender;
   
   public RowAppender(ValueEscaper escaper, Appendable appender) {
      this.appender = appender;
      this.escaper = escaper;
   }
   
   public void append(Row row) throws IOException {
      int count = row.getCount();
      
      for(int i = 0; i < count; i++) {
         Cell cell = row.getCell(i);
         Comparable value = cell.getValue();       
         
         if(i > 0) {
            appender.append(',');
         }
         if(value != null) {
            append(cell);
         }
      }
   }
   
   private void append(Cell cell) throws IOException {
      Comparable value = cell.getValue();
      
      if(value != null) {
         Column column = cell.getColumn();
         DataType data = column.getDataType();            
         String text = String.valueOf(value);
         Class type = data.getType();
               
         if(type == String.class) { // SYMBOL and TEXT
            text = escaper.escape(text); 
         }
         appender.append(text);
      }
   }   
}
