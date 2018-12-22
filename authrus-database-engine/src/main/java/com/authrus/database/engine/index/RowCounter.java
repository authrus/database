package com.authrus.database.engine.index;

import java.util.Iterator;

import com.authrus.database.engine.Row;

public class RowCounter {
   
   private final RowCursor cursor;
   
   public RowCounter(RowCursor cursor) {
      this.cursor = cursor;
   }
   
   public int count() {
      Iterator<Row> iterator = cursor.iterator();
      int count = 0;
      
      while(iterator.hasNext()) {
         Row row = iterator.next();
         
         if(row != null) {
            count++;
         }
      }
      return count;
   }
    

}
