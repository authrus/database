package com.authrus.database.terminal.console;

import java.util.Collection;

public class ConsoleTableBuilder {

   public static ConsoleTable create(String... schema) {
      if(schema.length == 0) {
         throw new IllegalArgumentException("At least one column must be specified");
      }
      return new ConsoleTable(schema);
   }
   
   public static ConsoleTable create(Collection<String> titles) {
      int size = titles.size();
      int index = 0;
      
      if(size == 0) {
         throw new IllegalArgumentException("At least one column must be specified");
      }
      String[] schema = new String[size];
      
      for(String title : titles) {
         schema[index++] = title;
      }
      return new ConsoleTable(schema);
   }
}
