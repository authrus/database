package com.authrus.database.engine.index;

import com.authrus.database.Column;
import com.authrus.database.PrimaryKey;

public class ColumnIndexBuilder {
   
   private final ClusterBuilder builder;
   private final PrimaryKey key;
   
   public ColumnIndexBuilder(PrimaryKey key) {
      this.builder = new ClusterBuilder();
      this.key = key;
   }

   public ColumnIndexUpdater create(Column column) {
      String match = column.getName();
      int count = key.getCount();
      
      for(int i = 0; i < count; i++) {
         Column next = key.getColumn(i);
         String name = next.getTitle();
         
         if(name.equals(match)) {
            if(count > 1) {
               return new ClusterIndex(builder, column);
            }
            return new KeyIndex(column);
         }
      }                     
      return new ClusterIndex(builder, column);      

   }
}
